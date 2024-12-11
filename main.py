import psycopg2
import pandas as pd
import requests 
import re
import json
import os
from datetime import datetime
from sqlalchemy import create_engine, text

# Global variable since we will be using it everywhere
current_date = datetime.now().strftime('%Y-%m-%d')

# 1. Connet to database from in prepare for processing 
def connect_to_database():
    """
    Connect to the PostgreSQL database and fetch data from the f_court table.
    Returns:
        engine: The SQLAlchemy engine object for connecting to the database.
        connection: A live database connection object for executing queries.
        federal_court: A DataFrame containing data from the f_court table.
    """
    try:
        # Create the PostgreSQL connection engine
        engine = create_engine("postgresql+psycopg2://*********:****@localhost:5432/spa******")
        connection = engine.connect()

        print("Connected to PostgreSQL database.")

        # Fetch data from the f_court table using pandas
        query = "SELECT * FROM f_court;"
        federal_court = pd.read_sql(text(query), connection)
        
        print(f"Fetched {len(federal_court)} rows from the f_court table.")

        return engine, connection, federal_court
    except Exception as e:
        print(f"Error connecting to database or fetching data: {e}")
        return None, None, None

# 2. Retrieve data from Goverment data source
def search_gov_data(dated_directory):
    try:
        url = "your_website"
        query_params = {
            "query": json.dumps({
                "by": "location",
                "page": 0,
                "description": "USA",
                "county": "all",
                "state": "US",
                "zip": "00000",
                "country": "US",
                "locationType": "country",
                "lat": 39.8283,
                "lng": -98.5795,
                "filters": "default"
            })
        }

        os.makedirs(dated_directory, exist_ok=True)
        json_file_path = os.path.join(dated_directory, "locations_data.json")
        xlsx_file_path = os.path.join(dated_directory, "Gov_location_data.xlsx")

        response = requests.get(url, params=query_params)
        if response.status_code == 200:
            try:
                data = response.json()
            except ValueError:
                print("Error decoding JSON response.")
                return pd.DataFrame()

            locations_data = data.get("results", {}).get("locations", [])
            if not locations_data:
                print("No locations data found in response.")
                return pd.DataFrame()

            with open(json_file_path, "w", encoding="utf-8") as json_file:
                json.dump(locations_data, json_file, indent=4)
            print(f"Locations data successfully saved to JSON file in folder {current_date}")

            df = pd.DataFrame(locations_data)
            if "zip" in df.columns:
                df["zip"] = df["zip"].apply(lambda x: str(int(x)).zfill(5) if pd.notnull(x) and str(x).isdigit() else None)
            df.to_excel(xlsx_file_path, index=False)
            print(f"Data successfully saved to Excel filein folder {current_date}")
            return df
        else:
            print("Failed to retrieve data. Status code:", response.status_code)
            return pd.DataFrame()
    except Exception as e:
        print("An unexpected error occurred:", e)
        return pd.DataFrame()
    
# 3. Clean and Merge Data, for backup information only
def clean_and_merge_data(federal_data, gov_data, deduplicate_columns, exclude_values, dated_directory):
    """
    Cleans government data, processes federal data, and merges both datasets.

    Args:
        federal_data (pd.DataFrame): DataFrame containing federal court data.
        gov_data (pd.DataFrame): DataFrame containing government court data.
        deduplicate_columns (list): List of column names for deduplication in gov_data.
        exclude_values (set): Set of values to filter out from 'CourtType' in gov_data.
        dated_directory (str): Directory where the output file will be saved.

    Returns:
        pd.DataFrame: Cleaned and merged DataFrame.
    """
    # Ensure the output directory exists
    os.makedirs(dated_directory, exist_ok=True)

    # Step 1: Clean gov_data
    print("Cleaning government data...")
    gov_data = gov_data[~gov_data['CourtType'].isin(exclude_values)]
    gov_data = gov_data.drop_duplicates(subset=deduplicate_columns)
    gov_data['Key'] = gov_data.apply(
        lambda row: f"{row['BuildingCity'].replace(' ', '_')}_{row['BuildingState']}".lower(), axis=1
    )
    print("Government data cleaning completed.")
    gov_cleaned_path = os.path.join(dated_directory, 'Gov_Data_Clean.xlsx')
    gov_data.to_excel(gov_cleaned_path, index=False, engine='openpyxl')
    print(f"Cleaned government data saved to folder {current_date}")
    
    # Step 2: Process federal_data
    print("Processing federal data...")
    federal_data['Desc'] = federal_data.apply(
        lambda row: 'Branch' if row['filingcity'] != row['city'] else 'Main', axis=1
    )
    federal_data['Key'] = federal_data.apply(
        lambda row: f"{row['filingcity'].replace(' ', '_')}_{row['state']}".lower(), axis=1
    )
    print("Federal data processing completed.")
        # Save cleaned gov_data
    fed_cleaned_path = os.path.join(dated_directory, 'Fed_Data_Clean.xlsx')
    federal_data.to_excel(fed_cleaned_path, index=False, engine='openpyxl')
    print(f"Cleaned federal court data saved to folder {current_date}")
    
    # Step 3: Merge federal_data and gov_data on the Key column
    print("Merging data...")
    merged_data = pd.merge(federal_data, gov_data, on='Key', how='outer', suffixes=('_Federal', '_Gov'))

    # Step 4: Save merged data to the specified path
    save_path = os.path.join(dated_directory, 'Gov_Federal_Merge_Data.xlsx')
    merged_data.to_excel(save_path, index=False, engine='openpyxl')
    print(f"Merged data saved to folder {current_date}")

    return federal_data,gov_data, merged_data

# 4. Data validation steps, Compare data from database(Federal Court Data) to New Goverment data
def is_valid_address(address):
    """
    Validates an address by checking if it starts with a number
    and is not empty or invalid formats like 'N/A', 'Unknown', etc.
    """
    if not address or not isinstance(address, str):
        return False
    normalized_address = address.strip()
    if not normalized_address[0].isdigit():
        return False
    if normalized_address in ("N/A", "Unknown", "None"):
        return False
    return True

def clean_address(address):
    """
    Cleans an address component by replacing commas with spaces, 
    removing periods, and trimming extra whitespace.
    Converts the address to lowercase for consistent comparison.
    Handles NaN values by returning an empty string.
    """
    if pd.isnull(address):  # Handle NaN or None
        return ""
    # Replace commas with spaces, remove periods, and trim extra whitespace
    cleaned_address = str(address).replace(",", " ").replace(".", "").strip()
    return re.sub(r"\s+", " ", cleaned_address).lower()  # Ensure single spaces between words

def format_address(address1, address2=None, city='', state='', zipcode=''):
    """
    Formats and normalizes addresses by combining components into a full address,
    cleaning each component, and ensuring consistent formatting.
    Includes normalization for common abbreviations, punctuation cleanup, and handling of missing values.
    """
    if not address1 or not city or not state or not zipcode:
        # Return default empty values if required fields are missing
        return '', ''

    # Common replacements for address normalization
    replacements = {
        r'\bSW\b': 'Southwest',
        r'\bNE\b': 'Northeast',
        r'\bNW\b': 'Northwest',
        r'\bSE\b': 'Southeast',
        r'\bN\b': 'North',
        r'\bS\b': 'South',
        r'\bE\b': 'East',
        r'\bW\b': 'West',
        r'\bSt\b': 'Street',
        r'\bAve\b': 'Avenue',
        r'\bRd\b': 'Road',
        r'\bBlvd\b': 'Boulevard',
        r'\bDr\b': 'Drive',
        r'\bLn\b': 'Lane',
        r'\bCt\b': 'Court',
        r'\bPl\b': 'Place',
        r'\bTerr\b': 'Terrace',
        r'\bPkwy\b': 'Parkway',
        r'\bHwy\b': 'Highway',
        r'\bSte\b': 'Suite',
        r'\bFl\b': 'Floor',
        r'\bBldg\b': 'Building',
        r'\bApt\b': 'Apartment',
        r'\bUnit\b': 'Unit',
        r'\b#\b': 'Unit',
    }

    def clean_and_normalize(part):
        """
        Cleans and normalizes a single address component.
        Handles NaN values, removes punctuation, and applies replacements.
        """
        if not part or pd.isnull(part):
            return ""
        # Remove commas, periods, and excess spaces
        cleaned_part = re.sub(r"[,.]", " ", str(part)).strip()
        # Replace abbreviations with full forms
        for pattern, replacement in replacements.items():
            cleaned_part = re.sub(pattern, replacement, cleaned_part, flags=re.IGNORECASE)
        # Normalize whitespace and return lowercase
        return re.sub(r"\s+", " ", cleaned_part).lower()

    # Clean and normalize all components
    address1 = clean_and_normalize(address1)
    address2 = clean_and_normalize(address2) if address2 else None
    city = clean_and_normalize(city)
    state = clean_and_normalize(state)
    zipcode = clean_and_normalize(zipcode)

    # Combine components into a list, ignoring empty values
    address_parts = [address1, address2, city, state, zipcode]
    formatted_address = ", ".join([part for part in address_parts if part])

    # Return the primary address and the full address (same format here)
    return formatted_address, formatted_address

def compare_data(federal_data, gov_data, dated_directory):
    """
    Compare Federal and Gov data to find matching entries based on the Key and Full_Address.

    Args:
        federal_data: DataFrame containing Federal Court data.
        gov_data: DataFrame containing Gov Court data.
        dated_directory: Directory to save the output result.

    Returns:
        federal_data: Updated Federal DataFrame with comparison results.
    """
    # Step 1: Format and clean Full_Address for Federal data
    federal_data['Full_Address'] = federal_data.apply(
        lambda row: format_address(
            row.get('address1', ''),  # Handle missing keys or None
            None,  # No address2 for federal data
            row.get('city', ''),
            row.get('state', ''),
            row.get('zipcode', '')
        )[0],  # Only primary address
        axis=1
    )

    # Step 2: Format and clean Full_Address for Gov data
    gov_data['Full_Address'] = gov_data.apply(
        lambda row: format_address(
            row.get('BuildingAddress', ''),  # Handle missing keys or None
            None,  # Exclude BuildingName for matching
            row.get('BuildingCity', ''),
            row.get('BuildingState', ''),
            str(row.get('BuildingZip', ''))[:5]  # Handle missing zip
        )[0],  # Only primary address
        axis=1
    )

    gov_data['Full_Address_With_BuildingName'] = gov_data.apply(
        lambda row: format_address(
            row.get('BuildingAddress', ''),  # Include BuildingName for address to update
            row.get('BuildingName', None),
            row.get('BuildingCity', ''),
            row.get('BuildingState', ''),
            str(row.get('BuildingZip', ''))[:5]
        )[1],  # Full address with BuildingName
        axis=1
    )

    # Step 3: Initialize columns for results in Federal data
    federal_data['Matched_in'] = 'Not Found'
    federal_data['Mismatch_Address'] = 'Manual Research'
    federal_data['Address_to_update'] = 'Human review needed'
    federal_data['Phone_to_update'] = ''

    # Step 4: Iterate over each Federal row and compare with Gov data
    for idx, federal_row in federal_data.iterrows():
        key = federal_row.get('Key', '')
        fed_address = federal_row['Full_Address']

        # Filter Gov data by Key
        gov_matches = gov_data[gov_data['Key'] == key]

        # Check for Bankruptcy Court matches
        gov_match_bankruptcy = gov_matches[gov_matches['CourtType'].str.lower() == 'bankruptcy court']

        if not gov_match_bankruptcy.empty:
            gov_row = gov_match_bankruptcy.iloc[0]
            gov_address = gov_row['Full_Address']
            gov_full_address_with_name = gov_row['Full_Address_With_BuildingName']

            if is_valid_address(gov_row['BuildingAddress']) and fed_address == gov_address:
                federal_data.loc[idx, 'Matched_in'] = 'Gov (Bankruptcy Court)'
                federal_data.loc[idx, 'Mismatch_Address'] = 'No mismatch'
                federal_data.loc[idx, 'Address_to_update'] = 'No update needed'
            else:
                federal_data.loc[idx, 'Matched_in'] = 'Gov (Bankruptcy Court)'
                federal_data.loc[idx, 'Mismatch_Address'] = f"Fed: {fed_address} | Gov: {gov_address}"
                federal_data.loc[idx, 'Address_to_update'] = gov_full_address_with_name
                federal_data.loc[idx, 'Phone_to_update'] = gov_row.get('Phone', '')
            continue  # Skip District Court check if Bankruptcy Court is matched

        # Check for District Court matches
        gov_match_district = gov_matches[gov_matches['CourtType'].str.lower() == 'district court']

        if not gov_match_district.empty:
            gov_row = gov_match_district.iloc[0]
            gov_address = gov_row['Full_Address']
            gov_full_address_with_name = gov_row['Full_Address_With_BuildingName']

            if is_valid_address(gov_row['BuildingAddress']) and fed_address == gov_address:
                federal_data.loc[idx, 'Matched_in'] = 'Gov (District Court)'
                federal_data.loc[idx, 'Mismatch_Address'] = 'No mismatch'
                federal_data.loc[idx, 'Address_to_update'] = 'No update needed'
            else:
                federal_data.loc[idx, 'Matched_in'] = 'Gov (District Court)'
                federal_data.loc[idx, 'Mismatch_Address'] = f"Fed: {fed_address} | Gov: {gov_address}"
                federal_data.loc[idx, 'Address_to_update'] = gov_full_address_with_name
                federal_data.loc[idx, 'Phone_to_update'] = gov_row.get('Phone', '')

    # Step 5: Save the updated Federal data to a file
    save_path = os.path.join(dated_directory, 'result.xlsx')
    federal_data.to_excel(save_path, index=False, engine='openpyxl')
    print(f"Comparison results saved to folder {current_date}")

    return federal_data

# 5. Export and Update Discrepancies
def update_discrepancies(connection, comparison_results):
    """
    Updates the database table `f_court` based on discrepancies found in the comparison results.

    Args:
        connection: SQLAlchemy connection object.
        comparison_results: DataFrame containing comparison results with columns:
                           [Mismatch_Address, Address_to_update, Phone_to_update, courtID]

    Returns:
        None
    """
    try:
        # Filter rows needing updates (excluding 'No mismatch' and 'Manual Research')
        discrepancies = comparison_results[
            ~comparison_results['Mismatch_Address'].isin(['No mismatch', 'Manual Research'])
        ]

        # Iterate over rows with discrepancies
        for _, row in discrepancies.iterrows():
            court_id = row['courtid']
            address_to_update = row['Address_to_update']
            phone_to_update = row['Phone_to_update']

            # Split Address_to_update into components
            address_parts = [part.strip() for part in address_to_update.split(',')]

            # Check the number of address components
            if len(address_parts) == 4:
                # Only update address1, city, state, zipcode, and phone
                address1 = address_parts[0]
                address2 = None  # No address2 available
                filingcity = address_parts[1]
                city = filingcity  # Assuming filingcity is the same as city
                state = address_parts[2]
                zipcode = address_parts[3]
            elif len(address_parts) >= 5:
                # Update all fields including address2
                address1 = address_parts[0]
                address2 = address_parts[1]
                filingcity = address_parts[2]
                city = filingcity  # Assuming filingcity is the same as city
                state = address_parts[3]
                zipcode = address_parts[4]
            else:
                # If fewer than 4 components, skip this row (log for review)
                print(f"Skipping update for courtid {court_id} due to insufficient address components.")
                continue

            # Update query for f_court table
            update_query = """
                UPDATE f_court
                SET address1 = :address1,
                    address2 = :address2,
                    filingcity = :filingcity,
                    city = :city,
                    state = :state,
                    zipcode = :zipcode,
                    phone = :phone
                WHERE courtid = :courtid;
            """

            # Execute the update query
            connection.execute(
                text(update_query),
                {
                    'address1': address1,
                    'address2': address2,
                    'filingcity': filingcity,
                    'city': city,
                    'state': state,
                    'zipcode': zipcode,
                    'phone': phone_to_update,
                    'courtid': court_id,
                }
            )

        # Commit the transaction
        connection.commit()
        print("Discrepancy updates completed successfully.")

    except Exception as e:
        # Roll back the transaction in case of an error
        connection.rollback()
        print(f"Error updating discrepancies: {e}")
            
# Main Function
def main():
    """
    Main function to orchestrate the workflow.
    """
    output_directory =  'your_local_path' 
    dated_directory = os.path.join(output_directory, current_date)

    # Step 1: Connect to the database
    engine, connection, federal_data = connect_to_database()
    if connection is None or federal_data is None:
        print("Failed to connect to the database or fetch data.")
        return

    # Step 2: Search for data from the government website
    gov_data  = search_gov_data(dated_directory)
 
    # 3. Clean and Merge Data
    # Define deduplication columns and exclusion values
    deduplicate_columns = ['Address', 'BuildingAddress', 'BuildingCity', 'BuildingName',
                        'BuildingState', 'BuildingZip']
    exclude_values = {'Appeals Court', 'Federal Defenders', 'Probation/Pretrial Services'}
    updated_fed_data, updated_gov_data, merged_data = clean_and_merge_data(federal_data, gov_data, deduplicate_columns, exclude_values,dated_directory)

    # Step 4: Compare the data
    results = compare_data(updated_fed_data, updated_gov_data,dated_directory)

    #Step 6: Update discrepancies in the database
    update_discrepancies(connection,results)

    print("Workflow completed.")

if __name__ == "__main__":
    main()
    

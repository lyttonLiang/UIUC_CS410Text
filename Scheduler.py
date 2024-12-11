import schedule
import time
from main import main

# Schedule the task
# You can also put the main.py into a system scheduler, or software like Power Automate 
schedule.every().day.at("18:58").do(main)  # Run daily at midnight

# Keep the scheduler running
if __name__ == "__main__":
    print("Scheduler is running... Press Ctrl+C to exit.")
    while True:
        schedule.run_pending()
        time.sleep(1)
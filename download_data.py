import kagglehub
import os
import pandas as pd
from datetime import datetime

# Configure Kaggle Dataset Slug
DATASET_SLUG = "mczielinski/bitcoin-historical-data"
DATA_DIR = "data"
MASTER_FILE = os.path.join(DATA_DIR, "btcusdt_analysis_data.csv")

def download_and_append():
    print(f"Downloading latest dataset version of '{DATASET_SLUG}' from Kagglehub...")
    # This downloads to a temporary/cache location
    path = kagglehub.dataset_download(DATASET_SLUG)
    print(f"Dataset downloaded to: {path}")

    # Find the main CSV file (btcusd_1-min_data.csv)
    csv_file = None
    for root, dirs, filenames in os.walk(path):
        for filename in filenames:
            if filename.endswith(".csv"):
                csv_file = os.path.join(root, filename)
                break
    
    if not csv_file:
        print("Error: No CSV file found in downloaded dataset.")
        return

    print(f"Loading data from: {csv_file}")
    # Load newly downloaded data
    # (Using chunking or just loading if it's manageable. 101MB is manageable for pandas in most cases)
    new_df = pd.read_csv(csv_file)
    print(f"Loaded {len(new_df)} rows from Kaggle.")

    # Load master file if it exists
    if os.path.exists(MASTER_FILE):
        print(f"Master file exists at {MASTER_FILE}. Merging new data...")
        master_df = pd.read_csv(MASTER_FILE)
        print(f"Loaded {len(master_df)} existing rows.")
        
        # Merge and drop duplicates based on 'Timestamp'
        combined_df = pd.concat([master_df, new_df])
        combined_df = combined_df.drop_duplicates(subset=['Timestamp'], keep='last')
        combined_df = combined_df.sort_values(by='Timestamp')
    else:
        print(f"Creating new master file at {MASTER_FILE}...")
        combined_df = new_df.sort_values(by='Timestamp')

    # Ensure data directory exists
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    # Save to master file
    combined_df.to_csv(MASTER_FILE, index=False)
    print(f"Successfully updated {MASTER_FILE}. Total rows: {len(combined_df)}")

if __name__ == "__main__":
    download_and_append()

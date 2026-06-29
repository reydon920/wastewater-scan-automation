import pandas as pd
from datetime import datetime
import os

def fetch_and_process_wastewater():
    # Live URL copied from your network tab
    csv_url = "https://data.wastewaterscan.org/data/averages/national.csv"
    
    print("Downloading live wastewater dataset...")
    # Read the live data into memory using pandas
    raw_df = pd.read_csv(csv_url)
    
    # Define your exact final target schema columns
    target_columns = [
        'sample_date', 'pathogen', 'concentration', 'concentration_units', 
        'pct_detectable', 'rolling_average', 'location_id', 'location_name', 
        'region', 'state'
    ]
    
    # 1. Clean and align headers if the source naming differs slightly
    # This ensures your dataset aligns with your target layout perfectly
    rename_dict = {
        'date': 'sample_date',
        'plant_id': 'location_id',
        'plant_name': 'location_name'
    }
    processed_df = raw_df.rename(columns=rename_dict)
    
    # 2. Check for missing columns and dynamically initialize them if needed
    for col in target_columns:
        if col not in processed_df.columns:
            processed_df[col] = None
            
    # Keep only your designated target columns
    final_df = processed_df[target_columns].copy()
    
    # 3. Inject tracking timestamps for your GitHub Action automation pipeline
    current_time = datetime.utcnow()
    final_df['fetched_at_utc'] = current_time.isoformat() + "+00:00"
    final_df['fetched_year'] = current_time.year
    final_df['fetched_month'] = current_time.month
    final_df['fetched_day'] = current_time.day
    
    # Ensure the local output folder directory structure exists
    os.makedirs("output", exist_ok=True)
    
    # Match your existing dynamic file naming convention
    filename = f"output/wastewater_{current_time.strftime('%Y%m%d')}.csv"
    
    # Export clean, un-nested CSV data
    final_df.to_csv(filename, index=False)
    print(f"Success! Exported fully populated data to {filename}")

if __name__ == "__main__":
    fetch_and_process_wastewater()

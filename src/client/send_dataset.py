import pandas as pd
import requests
import json
import time

# Read dataset
df = pd.read_csv("dataset/ncr_ride_bookings.csv").head(10000)

# API URL
url = "http://127.0.0.1:8000/generate-events"

BATCH_SIZE = 10

print(f"Dataset Loaded Successfully") 
print(f"Total Rows: {len(df)}")

while True:

    try:
        # Take user Input
        user_input = input( "\nEnter start and end row number (example: 51 100) or type 'exit': " )

        if user_input.lower() == "exit": 
            print("Stopping Producer...") 
            break

        start_row, end_row = map(int, user_input.split())

        # Validate input 
        if start_row < 1 or end_row > len(df) or start_row > end_row: 
            print("Invalid row range") 
            continue

        batch_input_df = df.iloc[start_row - 1:end_row]

        print(f"\nProcessing Rows {start_row} to {end_row}")

        # Send data in chunks
        for start in range(0, len(batch_input_df), BATCH_SIZE):

            batch_df = batch_input_df.iloc[start:start + BATCH_SIZE]

            payload = []

            for _, row in batch_df.iterrows():
                payload.append({
                    key: (None if pd.isna(value) else value)
                    for key, value in row.to_dict().items()
                })

            print(f"Sending batch with {len(payload)} rows")

            response = requests.post(url, json=payload)

            # print(json.dumps(response.json(), indent=2, default=str))

            time.sleep(5)

        print(f"\nCompleted sending rows {start_row} to {end_row}")

    except ValueError: 
        print("Please enter valid numbers")

    except Exception as e: 
        print(f"Error: {str(e)}")
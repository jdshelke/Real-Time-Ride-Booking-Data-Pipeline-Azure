import pandas as pd
import numpy as np
import requests
import json
import time

# Read dataset file 
df = pd.read_csv("dataset/ncr_ride_bookings.csv").head(10)

# API URL
url = "http://127.0.0.1:8000/generate-events"

for _, row in df.iterrows():

    # Create json payload
    payload = [{
        key: (None if pd.isna(value) else value)
        for key, value in row.to_dict().items()
    }]
    print(payload)

    response = requests.post(url, json=payload)

    print(json.dumps(response.json(), indent=2, default=str))

    time.sleep(5)
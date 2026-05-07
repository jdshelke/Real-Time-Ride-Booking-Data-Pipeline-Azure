from fastapi import FastAPI
from typing import List
import pandas as pd

# Import all methods from event_generator
from src.transformations.event_generator import (
    generate_completed_events,
    generate_incomplete_events,
    generate_no_driver_found_events,
    generate_driver_cancelled_events,
    generate_customer_cancelled_events
)

# Import event queue
from src.queue.event_queue import event_queue

app = FastAPI()

status_function_map = {
    "Completed": generate_completed_events,
    "Cancelled by Driver": generate_driver_cancelled_events,
    "No Driver Found": generate_no_driver_found_events,
    "Incomplete": generate_incomplete_events,
    "Cancelled by Customer": generate_customer_cancelled_events
}

@app.post("/generate-events")

def generate_events(rows: List[dict]):

    events_data_list = []

    for row_data in rows:

        row = pd.Series(row_data)

        base_event_row = {
            "Booking ID" : row["Booking ID"],
            "Booking Status" : row["Booking Status"],
            "Customer ID" : row["Customer ID"],
            "Vehicle Type" : row["Vehicle Type"],
            "Pickup Location" : row["Pickup Location"],
            "Drop Location" : row["Drop Location"],

            "Event Type": None,
            "Event Time": None,

            "Ride Distance": None,
            "Booking Value": None,
            "Payment Method": None,
            "Driver Ratings": None,
            "Customer Rating": None,

            "Cancelled by": None,
            "Cancellation Reason": None,
            "Incomplete Rides": None,
            "Incomplete Rides Reason": None
        }
        
        func = status_function_map.get(row["Booking Status"])

        if func:
            func(row, base_event_row, events_data_list)
        
    for event in events_data_list:
        event_queue.append(event)
        
    return {
        "total_generated_events": len(events_data_list),
        "events": "Events been added to Event Queue"
    }
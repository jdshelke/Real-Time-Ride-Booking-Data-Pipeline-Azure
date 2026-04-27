import pandas as pd
import random

def load_data(file_path):
    df = pd.read_csv(file_path).head(10001)
    return df

def generate_completed_events(row, event_row, events_data_list):

    events = ["BOOKING_CREATED", "DRIVER_ASSIGNED", "RIDE_STARTED", "RIDE_COMPLETED", "PAYMENT_COMPLETED", "RATING_SUBMITTED"]

    booking_time = pd.to_datetime(str(row["Date"]) + " " + str(row["Time"]))
    vtat = pd.to_timedelta(row["Avg VTAT"] if pd.notna(row["Avg VTAT"]) else 0, unit="m")
    ctat = pd.to_timedelta(row["Avg CTAT"] if pd.notna(row["Avg CTAT"]) else 0, unit="m")
    payment_time = pd.to_timedelta(random.randint(15, 300), unit="s")
    rating_time = pd.to_timedelta(random.randint(15, 300), unit="s")

    for event in events:
        row_copy = event_row.copy()
        
        if event == "BOOKING_CREATED":
            event_type = event
            event_time = booking_time
            row_copy["Event Type"] = event_type
            row_copy["Event Time"] = event_time
        elif event == "DRIVER_ASSIGNED":
            event_type = event
            event_time = booking_time + vtat
            row_copy["Event Type"] = event_type
            row_copy["Event Time"] = event_time
        elif event == "RIDE_STARTED":
            event_type = event
            event_time = booking_time + vtat
            row_copy["Event Type"] = event_type
            row_copy["Event Time"] = event_time
        elif event == "RIDE_COMPLETED":
            event_type = event
            event_time = booking_time + vtat + ctat
            ride_distance = row["Ride Distance"]
            row_copy["Event Type"] = event_type
            row_copy["Event Time"] = event_time
            row_copy["Ride Distance"] = ride_distance
        elif event == "PAYMENT_COMPLETED":
            event_type = event
            event_time = booking_time + vtat + ctat + payment_time
            booking_value = row["Booking Value"]
            payment_method = row["Payment Method"]
            row_copy["Event Type"] = event_type
            row_copy["Event Time"] = event_time
            row_copy["Booking Value"] = booking_value
            row_copy["Payment Method"] = payment_method
        elif event == "RATING_SUBMITTED":
            event_type = event
            event_time = booking_time + vtat + ctat + payment_time + rating_time
            driver_rating = row["Driver Ratings"]
            customer_rating = row["Customer Rating"]
            row_copy["Event Type"] = event_type
            row_copy["Event Time"] = event_time
            row_copy["Driver Ratings"] = driver_rating
            row_copy["Customer Rating"] = customer_rating
        
        events_data_list.append(row_copy)

    return None

def generate_driver_cancelled_events(row, event_row, events_data_list):

    events = ["BOOKING_CREATED", "DRIVER_ASSIGNED", "DRIVER_CANCELLED"]

    booking_time = pd.to_datetime(str(row["Date"]) + " " + str(row["Time"]))
    vtat = pd.to_timedelta(row["Avg VTAT"] if pd.notna(row["Avg VTAT"]) else 0, unit="m")
    cancel_time = pd.to_timedelta(random.randint(15, 900), unit="s")
    for event in events:
        row_copy = event_row.copy()
        if event == "BOOKING_CREATED":
            event_type = event
            event_time = booking_time
            row_copy["Event Type"] = event_type
            row_copy["Event Time"] = event_time
        elif event == "DRIVER_ASSIGNED":
            event_type = event
            event_time = booking_time + vtat
            row_copy["Event Type"] = event_type
            row_copy["Event Time"] = event_time
        elif event == "DRIVER_CANCELLED":
            event_type = event
            event_time = booking_time + vtat + cancel_time
            cancelled_by = "Driver"
            cancellation_reason = row["Driver Cancellation Reason"]
            row_copy["Event Type"] = event_type
            row_copy["Event Time"] = event_time
            row_copy["Cancelled by"] = cancelled_by
            row_copy["Cancellation Reason"] = cancellation_reason
        
        events_data_list.append(row_copy)

    return None

def generate_no_driver_found_events(row, event_row, events_data_list):

    event = "BOOKING_CREATED"

    booking_time = pd.to_datetime(str(row["Date"]) + " " + str(row["Time"]))

    row_copy = event_row.copy()

    event_type = event
    event_time = booking_time
    row_copy["Event Type"] = event_type
    row_copy["Event Time"] = event_time
        
    events_data_list.append(row_copy)

    return None

def generate_incomplete_events(row, event_row, events_data_list):

    events = ["BOOKING_CREATED", "DRIVER_ASSIGNED", "RIDE_STARTED", "RIDE_INCOMPLETE", "PAYMENT_COMPLETED"]

    booking_time = pd.to_datetime(str(row["Date"]) + " " + str(row["Time"]))
    vtat = pd.to_timedelta(row["Avg VTAT"] if pd.notna(row["Avg VTAT"]) else 0, unit="m")
    ctat = pd.to_timedelta(row["Avg CTAT"] if pd.notna(row["Avg CTAT"]) else 0, unit="m")
    ride_incomplete_time = pd.to_timedelta(random.randint(120, 1500), unit="s")
    payment_time = pd.to_timedelta(random.randint(15, 300), unit="s")
    for event in events:
        row_copy = event_row.copy()
        if event == "BOOKING_CREATED":
            event_type = event
            event_time = booking_time
            row_copy["Event Type"] = event_type
            row_copy["Event Time"] = event_time
        elif event == "DRIVER_ASSIGNED":
            event_type = event
            event_time = booking_time + vtat
            row_copy["Event Type"] = event_type
            row_copy["Event Time"] = event_time
        elif event == "RIDE_STARTED":
            event_type = event
            event_time = booking_time + vtat
            row_copy["Event Type"] = event_type
            row_copy["Event Time"] = event_time
        elif event == "RIDE_INCOMPLETE":
            event_type = event
            event_time = booking_time + vtat + ride_incomplete_time
            incomplete_rides = 1
            incomplete_reason  = row["Incomplete Rides Reason"]
            ride_distance = row["Ride Distance"]
            row_copy["Event Type"] = event_type
            row_copy["Event Time"] = event_time
            row_copy["Incomplete Rides"] = incomplete_rides
            row_copy["Incomplete Rides Reason"] = incomplete_reason
            row_copy["Ride Distance"] = ride_distance
        elif event == "PAYMENT_COMPLETED":
            event_type = event
            event_time = booking_time + vtat + ctat + payment_time
            booking_value = row["Booking Value"]
            payment_method = row["Payment Method"]
            row_copy["Event Type"] = event_type
            row_copy["Event Time"] = event_time
            row_copy["Booking Value"] = booking_value
            row_copy["Payment Method"] = payment_method
        
        events_data_list.append(row_copy)

    return None

def generate_customer_cancelled_events(row, event_row, events_data_list):

    events = ["BOOKING_CREATED", "DRIVER_ASSIGNED", "CUSTOMER_CANCELLED"]

    booking_time = pd.to_datetime(str(row["Date"]) + " " + str(row["Time"]))
    vtat = pd.to_timedelta(row["Avg VTAT"] if pd.notna(row["Avg VTAT"]) else 0, unit="m")
    cancel_time = pd.to_timedelta(random.randint(15, 900), unit="s")
    for event in events:
        row_copy = event_row.copy()
        if event == "BOOKING_CREATED":
            event_type = event
            event_time = booking_time
            row_copy["Event Type"] = event_type
            row_copy["Event Time"] = event_time
        elif event == "DRIVER_ASSIGNED":
            event_type = event
            event_time = booking_time + vtat
            row_copy["Event Type"] = event_type
            row_copy["Event Time"] = event_time
        elif event == "CUSTOMER_CANCELLED":
            event_type = event
            event_time = booking_time + vtat + cancel_time
            cancelled_by = "Customer"
            cancellation_reason = row["Reason for cancelling by Customer"]
            row_copy["Event Type"] = event_type
            row_copy["Event Time"] = event_time
            row_copy["Cancelled by"] = cancelled_by
            row_copy["Cancellation Reason"] = cancellation_reason
        
        events_data_list.append(row_copy)

    return None


if __name__ == "__main__":

    events_data_list = []

    status_function_map = {
        "Completed": generate_completed_events,
        "Cancelled by Driver": generate_driver_cancelled_events,
        "No Driver Found": generate_no_driver_found_events,
        "Incomplete": generate_incomplete_events,
        "Cancelled by Customer": generate_customer_cancelled_events
    }

    df = load_data("dataset/ncr_ride_bookings.csv")

    for index, row in df.iterrows():

        booking_id = row["Booking ID"]
        booking_status = row["Booking Status"]
        customer_id = row["Customer ID"]
        vehicle_type = row["Vehicle Type"]
        pickup_location = row["Pickup Location"]
        drop_location = row["Drop Location"]

        base_event_row = {
            "Booking ID" : booking_id,
            "Booking Status" : booking_status,
            "Customer ID" : customer_id,
            "Vehicle Type" : vehicle_type,
            "Pickup Location" : pickup_location,
            "Drop Location" : drop_location,

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

    events_df = pd.DataFrame(events_data_list)
    events_df.to_csv("data/raw/event_data.csv", index=False)
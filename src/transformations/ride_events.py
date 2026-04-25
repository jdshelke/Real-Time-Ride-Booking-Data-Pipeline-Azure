import pandas as pd
import random

def load_data(file_path):
    df = pd.read_csv(file_path)
    df_completed = df[df["Booking Status"] == "Completed"]

    events = ["BOOKING_CREATED", "DRIVER_ASSIGNED", "RIDE_STARTED", "RIDE_COMPLETED", "PAYMENT_COMPLETED", "RATING_SUBMITTED"]

    events_list = []
    for index, row in df_completed.iterrows():
        booking_id = row["Booking ID"]
        customer_id = row["Customer ID"]
        vehicle_type = row["Vehicle Type"]
        pickup_location = row["Pickup Location"]
        drop_location = row["Drop Location"]
        booking_status = row["Booking Status"]

        event_row = {
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
            
            events_list.append(row_copy)

    events_df = pd.DataFrame(events_list)
    events_df.to_csv("data/raw/event_data.csv", index=False)


    return events_df

df = load_data("dataset/ncr_ride_bookings.csv")

print(df.head())
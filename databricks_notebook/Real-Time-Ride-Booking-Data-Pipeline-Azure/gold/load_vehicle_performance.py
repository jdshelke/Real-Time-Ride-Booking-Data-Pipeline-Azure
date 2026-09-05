# Databricks notebook source
vehicle_performance_df = spark.sql("""

SELECT
    vehicle_type,

    COUNT(*) AS total_rides,

    SUM(
        CASE
            WHEN booking_status = 'Completed'
            THEN 1
            ELSE 0
        END
    ) AS completed_rides,

    SUM(
        CASE
            WHEN booking_status IN (
                'Cancelled by Driver',
                'Cancelled by Customer'
            )
            THEN 1
            ELSE 0
        END
    ) AS cancelled_rides,

    SUM(
        CASE
            WHEN booking_status = 'Incomplete'
            THEN 1
            ELSE 0
        END
    ) AS incomplete_rides,

    SUM(
        CASE
            WHEN booking_status = 'No Driver Found'
            THEN 1
            ELSE 0
        END
    ) AS no_driver_found_rides,

    ROUND(
        AVG(
            CASE
                WHEN ride_distance > 0
                THEN ride_distance
            END
        ),
        2
    ) AS avg_distance,

    ROUND(
        SUM(
            CASE
                WHEN booking_status IN (
                    'Completed',
                    'Incomplete'
                )
                THEN booking_value
                ELSE 0
            END
        ),
        2
    ) AS total_revenue,

    ROUND(
        AVG(
            CASE
                WHEN booking_status IN (
                    'Completed',
                    'Incomplete'
                )
                THEN booking_value
            END
        ),
        2
    ) AS avg_fare,

    CAST(
        ROUND(
            (
                SUM(
                    CASE
                        WHEN booking_status = 'Completed'
                        THEN 1
                        ELSE 0
                    END
                ) * 100.0
            ) / COUNT(*),
            2
        ) AS DOUBLE
    ) AS completion_rate_percentage,

    CURRENT_TIMESTAMP() AS created_time

FROM real_time_ride_booking_data_pipeline_azure.silver.uber_ride_bookings

GROUP BY vehicle_type

""")

# COMMAND ----------

vehicle_performance_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "real_time_ride_booking_data_pipeline_azure.gold.vehicle_performance"
    )
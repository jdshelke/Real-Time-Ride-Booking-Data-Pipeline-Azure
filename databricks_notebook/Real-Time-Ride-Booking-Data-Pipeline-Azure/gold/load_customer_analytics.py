# Databricks notebook source
customer_analytics_df = spark.sql("""

SELECT
    customer_id,

    COUNT(*) AS total_bookings,

    SUM(
        CASE
            WHEN booking_status = 'Completed'
            THEN 1
            ELSE 0
        END
    ) AS completed_bookings,

    SUM(
        CASE
            WHEN booking_status IN (
                'Cancelled by Driver',
                'Cancelled by Customer'
            )
            THEN 1
            ELSE 0
        END
    ) AS cancelled_bookings,

    SUM(
        CASE
            WHEN booking_status = 'Incomplete'
            THEN 1
            ELSE 0
        END
    ) AS incomplete_bookings,

    SUM(
        CASE
            WHEN booking_status = 'No Driver Found'
            THEN 1
            ELSE 0
        END
    ) AS no_driver_found_bookings,

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
    ) AS total_spend,

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
    ) AS avg_booking_value,

    ROUND(
        AVG(
            CASE
                WHEN customer_rating > 0
                THEN customer_rating
            END
        ),
        2
    ) AS avg_customer_rating,

    ROUND(
        AVG(
            CASE
                WHEN driver_rating > 0
                THEN driver_rating
            END
        ),
        2
    ) AS avg_driver_rating,

    CURRENT_TIMESTAMP() AS created_time

FROM real_time_ride_booking_data_pipeline_azure.silver.uber_ride_bookings

GROUP BY customer_id

""")

# COMMAND ----------

customer_analytics_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "real_time_ride_booking_data_pipeline_azure.gold.customer_analytics"
    )
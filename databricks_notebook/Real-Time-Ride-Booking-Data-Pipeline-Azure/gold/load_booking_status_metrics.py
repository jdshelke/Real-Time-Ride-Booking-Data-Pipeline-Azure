# Databricks notebook source
booking_status_metrics_df = spark.sql("""

SELECT

    COUNT(*) AS total_rides,

    SUM(
        CASE
            WHEN booking_status = 'Completed'
            THEN 1
            ELSE 0
        END
    ) AS completed_count,

    SUM(
        CASE
            WHEN booking_status IN (
                'Cancelled by Driver',
                'Cancelled by Customer'
            )
            THEN 1
            ELSE 0
        END
    ) AS cancelled_count,

    SUM(
        CASE
            WHEN booking_status = 'Incomplete'
            THEN 1
            ELSE 0
        END
    ) AS incomplete_count,

    SUM(
        CASE
            WHEN booking_status = 'No Driver Found'
            THEN 1
            ELSE 0
        END
    ) AS no_driver_found_count,

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
    ) AS completed_percentage,

    CAST(
        ROUND(
            (
                SUM(
                    CASE
                        WHEN booking_status IN (
                            'Cancelled by Driver',
                            'Cancelled by Customer'
                        )
                        THEN 1
                        ELSE 0
                    END
                ) * 100.0
            ) / COUNT(*),
            2
        ) AS DOUBLE
    ) AS cancelled_percentage,

    CAST(
        ROUND(
            (
                SUM(
                    CASE
                        WHEN booking_status = 'Incomplete'
                        THEN 1
                        ELSE 0
                    END
                ) * 100.0
            ) / COUNT(*),
            2
        ) AS DOUBLE
    ) AS incomplete_percentage,

    CAST(
        ROUND(
            (
                SUM(
                    CASE
                        WHEN booking_status = 'No Driver Found'
                        THEN 1
                        ELSE 0
                    END
                ) * 100.0
            ) / COUNT(*),
            2
        ) AS DOUBLE
    ) AS no_driver_found_percentage,

    CAST(
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
        ) AS DOUBLE
    ) AS total_revenue,

    CURRENT_TIMESTAMP() AS created_time

FROM real_time_ride_booking_data_pipeline_azure.silver.uber_ride_bookings

""")

# COMMAND ----------

booking_status_metrics_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "real_time_ride_booking_data_pipeline_azure.gold.booking_status_metrics"
    )
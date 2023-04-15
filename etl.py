# pylint:  disable-all
"""
    This project gather data from two sources: the New York City Taxi and Limousine Commission's (TLC) trip record data 
    and the weather data scraped from https://www.wunderground.com/ 
    
    - With a star schema based on the TLC Trip Record Data  and weather data, you can perform a variety of analytical queries to gain insights into for-hire vehicle industry in New York City. 
   
    - Fact Table: Trips
        trip_id (Primary key)
        hvfhs_license_num (Foreign key referencing HVFHS)
        dispatching_base_num (Foreign key referencing base_num)
        originating_base_num (Foreign key referencing base_num)
        request_datetime (Foreign key referencing DateTimes)
        pickup_datetime (Foreign key referencing DateTimes)
        dropoff_datetime (Foreign key referencing DateTimes)
        PULocationID (Foreign key referencing Locations)
        DOLocationID (Foreign key referencing Locations)
        weather_id
        trip_miles
        trip_time
        base_passenger_fare
        tolls
        bcf
        sales_tax
        congestion_surcharge
        airport_fee
        tips
        driver_pay
        shared_request_flag
        shared_match_flag

    - Dimension Tables:
        - HVL
            hv_license_num (Primary key)
            affiliation
            base_num (Primary key)
            base_name

        - DateTimes
            datetime_id (Primary key)
            full_datetime
            date
            hour
            day
            month
            week
            day_of_week

        - Locations
            location_id (Primary key)
            borough
            zone
            service_zone
        
        - weather 
            weather_id	
            date	
            time	
            temperature		
            humidity	
            wind_speed			
            condition

"""

import configparser
import glob
import os
from datetime import datetime

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    date_format,
    dayofmonth,
    dayofweek,
    from_unixtime,
    hour,
    lit,
    minute,
    month,
    row_number,
    split,
    sum,
    to_timestamp,
    udf,
    unix_timestamp,
    when,
    year,
)
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.window import Window
from utils import data_quality

config = configparser.ConfigParser()
config.read("dl.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config["AWS"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS"]["AWS_SECRET_ACCESS_KEY"]


def create_spark_session():
    """
    Creates a SparkSession object and returns it.

    Returns:
        A SparkSession object.
    """
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    return spark


def process_location_data(spark, input_data, output_data):
    """
    Processes location data using Spark and writes the resulting Parquet files to S3.

    Args:
        spark: A SparkSession object.
        input_data: A string representing the input data path.
        output_data: A string representing the output data path.

    Returns:
        None.
    """
    # get filepath to location data file
    loc_data = input_data + "tlc/zone_lookup.csv"
    # read location data
    df_loc = spark.read.csv(loc_data, header=True)

    # qulaity check
    loc_columns = ["location_id", "borough", "zone", "service_zone"]
    data_quality.contain_target_columns(df_loc, loc_columns, "location")

    data_quality.contain_null(df_loc, loc_columns, "location")
    data_quality.contain_rows(df_loc, "location")
    # write loaction table to parquet files
    df_loc.write.mode("overwrite").parquet(
        output_data + "location/location_table.parquet"
    )


def process_hvfhs_data(spark, input_data, output_data):

    """
    Processes hvfhs data using Spark and writes the resulting Parquet files to S3.

    Args:
        spark: A SparkSession object.
        input_data: A string representing the input data path.
        output_data: A string representing the output data path.

    Returns:
        None.
    """

    # get filepath to hvfhs data file
    hvl_data = input_data + "tlc/hvl_data.csv"
    # read hvfhs data
    df_hvl = spark.read.csv(hvl_data, header=True)

    # data quality
    hvl_cols = ["hv_license_number", "license_number", "base_name","affiliation" ]
    data_quality.contain_target_columns(df_hvl, hvl_cols, "hvl")

    data_quality.contain_null(df_hvl, hvl_cols, "hvl")
    data_quality.contain_rows(df_hvl, "hvl")

    df_hvl = df_hvl.select(
        col("hv_license_number"),
        col("license_number").alias("base_num"),
        col("base_name"),
        col("affiliation"),
    ).drop_duplicates()

    # write hvfhs table to parquet files
    df_hvl.write.mode("overwrite").parquet(output_data + "hvl/hvl_table.parquet")


def convert_time(time_str):
    """
    Converts the time from a string to an integer in 24-hour format.

    Args:
        time_str: A string representing the time in 12-hour format.

    Returns:
        A string representing the time in 24-hour format.
    """
    hour = int(time_str.split(":")[0])
    if "PM" in time_str:
        hour += 12
    if hour == 12 or hour == 24:  # Special case: 12 PM or 12 AM
        hour -= 12
    return f"{hour:02d}"


def process_weather_data(spark, input_data, output_data):

    """
    Processes weather data using Spark and writes the resulting Parquet files to S3.

    Args:
        spark: A SparkSession object.
        input_data: A string representing the input data path.
        output_data: A string representing the output data path.

    Returns:
        None.
    """
    weather_data = input_data + "weather/*.csv"

    # Define UDFs
    convert_time_udf = udf(lambda time_str: convert_time(time_str), StringType())
    split_udf = udf(lambda x: int(x.split(" ")[0]), IntegerType())

    # read data
    weather_df = spark.read.csv(weather_data, header=True, inferSchema=True)

    # data quality
    weather_cols = ["date",
            "time",
            "temperature",
            "humidity",
            "wind_speed",
            "condition" ]
    data_quality.contain_target_columns(weather_df, weather_cols, "weather")

    data_quality.contain_null(weather_df, weather_cols, "weather")
    data_quality.contain_rows(weather_df, "weather")


    # Read CSV files and apply transformations
   
    weather_df =( weather_df.withColumn("time", convert_time_udf("time"))
        .withColumn("date", date_format("date", "yyyy-MM-dd"))
        .withColumn("weather_id", concat_ws(" ", "date", "time"))
        .drop("dew_point", "wind", "wind_gust", "pressure", "precip")
        .withColumn("humidity", split_udf("humidity"))
        .withColumn("wind_speed", split_udf("wind_speed"))
        .withColumn("temperature", split_udf("temperature"))
        .select(
            "weather_id",
            "date",
            "time",
            "temperature",
            "humidity",
            "wind_speed",
            "condition",
        )
        .drop_duplicates(["weather_id"])
    )

    # Write output to parquet
    weather_df.write.mode("overwrite").parquet(
        output_data + "weather/weather_table.parquet"
    )


def process_datetime_data(spark, input_data, output_data):
    """
    Extracts datetime information from taxi trip data and writes it to a parquet file.

    Args:
        spark (SparkSession): Spark session object.
        input_data (str): Path to input data directory containing taxi trip data in parquet format.
        output_data (str): Path to output data directory where the datetime table will be saved.

    Returns:
        None

    """
    # get trip filepath
    trip_data = input_data + "tlc/*.parquet"
    # read trip data
    df_trip = spark.read.parquet(trip_data)

    # extract columns to create datetime table

    weekend_days = ["Sat", "Sun"]
    pickup_datetime = (
        df_trip.select(col("pickup_datetime").alias("datetime_id"))
        .withColumn("year", year(col("datetime_id")))
        .withColumn("month", month(col("datetime_id")))
        .withColumn("dayofmonth", dayofmonth(col("datetime_id")))
        .withColumn("dow", dayofweek(col("datetime_id")))
        .withColumn("hour", hour(col("datetime_id")))
        .withColumn("minute", minute(col("datetime_id")))
        .withColumn(
            "is_weekend",
            when(
                date_format(col("datetime_id"), "EEE").isin(weekend_days), 1
            ).otherwise(0),
        )
    )

    dropoff_datetime = (
        df_trip.select(col("dropoff_datetime").alias("datetime_id"))
        .withColumn("year", year(col("datetime_id")))
        .withColumn("month", month(col("datetime_id")))
        .withColumn("dom", dayofmonth(col("datetime_id")))
        .withColumn("dow", dayofweek(col("datetime_id")))
        .withColumn("hour", hour(col("datetime_id")))
        .withColumn("minute", minute(col("datetime_id")))
        .withColumn(
            "is_weekend",
            when(
                date_format(col("datetime_id"), "EEE").isin(weekend_days), 1
            ).otherwise(0),
        )
    )

    request_datetime = (
        df_trip.select(col("request_datetime").alias("datetime_id"))
        .withColumn("year", year(col("datetime_id")))
        .withColumn("month", month(col("datetime_id")))
        .withColumn("dom", dayofmonth(col("datetime_id")))
        .withColumn("dow", dayofweek(col("datetime_id")))
        .withColumn("hour", hour(col("datetime_id")))
        .withColumn("minute", minute(col("datetime_id")))
        .withColumn(
            "is_weekend",
            when(
                date_format(col("datetime_id"), "EEE").isin(weekend_days), 1
            ).otherwise(0),
        )
    )

    df_datetime = dropoff_datetime.union(dropoff_datetime)
    df_datetime = df_datetime.union(request_datetime)

    # write time table to parquet files partitioned by year and month
    df_datetime = df_datetime.write.mode("overwrite").parquet(
        output_data + "time/datetime_table.parquet"
    )


def process_trip_data(spark, input_data, output_data):
    """
    Processes trip data using Spark and writes the resulting Parquet files to S3.
    Args:
        spark: A SparkSession object.
        input_data: A string representing the input data path.
        output_data: A string representing the output data path.

    Returns:
        None.
    """
    # get trip filepath
    trip_data = input_data + "tlc/*.parquet"
    # read trip data
    df_trip = spark.read.parquet(trip_data)

    trip_cols = ["hvfhs_license_num",
        "dispatching_base_num",
        "originating_base_num",
        "request_datetime",
        "pickup_datetime",
        "dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "trip_miles",
        "trip_time",
        "base_passenger_fare",
        "tolls",
        "bcf",
        "sales_tax",
        "congestion_surcharge",
        "airport_fee",
        "tips",
        "driver_pay",
        "shared_request_flag",
        "shared_match_flag"]
    data_quality.contain_target_columns(df_trip, trip_cols, "trip")
    data_quality.contain_rows(df_trip, "trip")


    # extract trip columns
    df_trip = df_trip.select(
        "hvfhs_license_num",
        "dispatching_base_num",
        "originating_base_num",
        "request_datetime",
        "pickup_datetime",
        "dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "trip_miles",
        "trip_time",
        "base_passenger_fare",
        "tolls",
        "bcf",
        "sales_tax",
        "congestion_surcharge",
        "airport_fee",
        "tips",
        "driver_pay",
        "shared_request_flag",
        "shared_match_flag",
    )
    df_trip = df_trip.drop_duplicates()

    # fillna in originating_base_num with dispatching_base_num column and add weaher column
    df_trip = df_trip.withColumn(
        "originating_base_num", coalesce("originating_base_num", "dispatching_base_num")
    ).withColumn(
        "weather",
        date_format(
            from_unixtime((unix_timestamp("request_datetime") / 3600) * 3600),
            "yyyy-MM-dd HH",
        ),
    )
    # check if ather columns contain null valuesspa
    data_quality.contain_null(df_trip, trip_cols, "trip")

    # add trip_id column
    w = Window().orderBy(lit("A"))
    df_trip = df_trip.withColumn("trip_id", row_number().over(w))

    # write trip table to parquet files partitioned by year and month
    df_trip = df_trip.withColumn("year", year("request_datetime")).withColumn(
        "month", month("request_datetime")
    )

    df_trip.write.partitionBy("year", "month").mode("overwrite").parquet(
        output_data + "trip/trip_table.parquet"
    )


def main():
    spark = create_spark_session()
    input_data = "s3a://nyc-fhv-trips-and-weather-etl/data/"
    output_data = "s3a://nyc-fhv-trips-and-weather-data-lake/"

    process_location_data(spark, input_data, output_data)
    process_hvfhs_data(spark, input_data, output_data)
    process_datetime_data(spark, input_data, output_data)
    process_trip_data(spark, input_data, output_data)
    process_weather_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

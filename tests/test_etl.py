import pytest
from pyspark.sql import SparkSession
from etl import create_spark_session, process_location_data, process_hvfhs_data, process_weather_data, process_datetime_data, process_trip_data

def test_create_spark_session():
    spark = create_spark_session()
    assert isinstance(spark, SparkSession), "create_spark_session should return a SparkSession object"

def test_process_location_data():
    spark = create_spark_session()
    input_data = "tests/test_data/"
    output_data = "tests/test_output/"
    df = process_location_data(spark, input_data, output_data)
    assert isinstance(df, DataFrame), "Output should be a DataFrame"
    assert set(df.columns) == {"expected", "columns"}, "Output should have the expected columns"

def test_process_hvfhs_data():
    spark = create_spark_session()
    input_data = "tests/test_data/"
    output_data = "tests/test_output/"
    df = process_hvfhs_data(spark, input_data, output_data)
    assert isinstance(df, DataFrame), "Output should be a DataFrame"
    assert set(df.columns) == {"expected", "columns"}, "Output should have the expected columns"

def test_process_weather_data():
    spark = create_spark_session()
    input_data = "tests/test_data/"
    output_data = "tests/test_output/"
    df = process_weather_data(spark, input_data, output_data)
    assert isinstance(df, DataFrame), "Output should be a DataFrame"
    assert set(df.columns) == {"expected", "columns"}, "Output should have the expected columns"

def test_process_datetime_data():
    spark = create_spark_session()
    input_data = "tests/test_data/"
    output_data = "tests/test_output/"
    df = process_datetime_data(spark, input_data, output_data)
    assert isinstance(df, DataFrame), "Output should be a DataFrame"
    assert set(df.columns) == {"expected", "columns"}, "Output should have the expected columns"

def test_process_trip_data():
    spark = create_spark_session()
    input_data = "tests/test_data/"
    output_data = "tests/test_output/"
    df = process_trip_data(spark, input_data, output_data)
    assert isinstance(df, DataFrame), "Output should be a DataFrame"
    assert set(df.columns) == {"expected", "columns"}, "Output should have the expected columns"
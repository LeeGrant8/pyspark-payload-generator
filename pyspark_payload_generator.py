"""
PySpark API Payload Generator

A module for transforming PySpark DataFrames into chunked API payloads
with metadata-driven field mapping.
"""

import os
import uuid
from datetime import datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, floor


def initialize_spark(app_name="PayloadGenerator"):
    """
    Initialize Spark session with proper configuration.

    Parameters:
    app_name: Name of the Spark application

    Returns:
    SparkSession object
    """
    # Set environment variables
    os.environ['SPARK_HOME'] = os.path.join(os.path.dirname(__import__('pyspark').__file__))
    os.environ['PYSPARK_PYTHON'] = 'python3'
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'python3'

    # Create SparkContext first
    conf = SparkConf()
    conf.setAppName(app_name)
    conf.setMaster("local[*]")

    try:
        # Stop any existing context
        SparkContext.getOrCreate().stop()
    except:
        pass

    # Create new context and session
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    print(f"Spark session initialized - Version: {spark.version}")
    return spark


def chunk_dataframe(df, chunk_size):
    """
    Split a PySpark dataframe into chunks of specified size.
    Optimized for distributed processing.

    Parameters:
    df: PySpark DataFrame to chunk
    chunk_size: Number of rows per chunk (e.g., 1000)

    Returns:
    List of PySpark DataFrames, each with chunk_size rows (except possibly the last one)
    """
    # Get total number of rows
    total_rows = df.count()

    # Calculate number of chunks needed
    num_chunks = (total_rows + chunk_size - 1) // chunk_size  # Ceiling division

    # Add a chunk_id column based on monotonically_increasing_id
    # This function is designed for distributed processing
    df_with_chunk = df.withColumn("_row_id", monotonically_increasing_id())
    df_with_chunk = df_with_chunk.withColumn("_chunk_id", floor(df_with_chunk._row_id / chunk_size))

    # Create list to store chunks
    chunks = []

    # Create each chunk by filtering on chunk_id
    for i in range(num_chunks):
        chunk = df_with_chunk.filter(df_with_chunk._chunk_id == i).drop("_row_id", "_chunk_id")
        chunks.append(chunk)

    print(f"Total rows: {total_rows}")
    print(f"Chunk size: {chunk_size}")
    print(f"Number of chunks: {num_chunks}")
    print(f"Last chunk size: {total_rows % chunk_size if total_rows % chunk_size != 0 else chunk_size}")

    return chunks


def build_work_request():
    """
    Builds a standard work request dictionary with default values.

    Returns:
    Dictionary containing the workRequest structure
    """
    # Generate a GUID (removing hyphens to match the format shown)
    work_request_id = str(uuid.uuid4()).replace('-', '').upper()

    # Get current datetime
    now = datetime.now()

    # Format dates
    date_period = now.strftime("%Y%m%d")  # YYYYMMDD
    extract_timestamp = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"  # YYYY-MM-DDThh:mm:ss.sZ

    work_request = {
        "workRequestId": work_request_id,
        "type": "BATTERS",
        "mode": "ASYNC",
        "load": "SNAPSHOT",
        "systemCode": "TEST",
        "dataPeriodStartDate": date_period,
        "extractTimestamp": extract_timestamp,
        "skipFlag": "N",
        "pagination": {
            "currentPage": 1,
            "pageSize": 50,
            "totalRecords": 500,
            "totalPages": 10
        }
    }

    return work_request


def map_row_to_structure(row, metadata):
    """
    Recursively maps a dataframe row to the JSON structure defined by metadata.

    Parameters:
    row: PySpark Row object
    metadata: Dictionary mapping JSON keys to column names (or nested structures)

    Returns:
    Dictionary with mapped values
    """
    result = {}

    for json_key, value in metadata.items():
        if isinstance(value, dict):
            # Nested structure - recurse
            result[json_key] = map_row_to_structure(row, value)
        else:
            # value is a column name - get the value from the row
            result[json_key] = row[value]

    return result


def build_payloads(work_request, chunks, metadata, array_name="batters"):
    """
    Builds API payloads from dataframe chunks using metadata mapping.
    Updates pagination for each payload during construction.

    Parameters:
    work_request: Base work request dictionary
    chunks: List of PySpark DataFrames (chunked data)
    metadata: Dictionary mapping JSON keys to dataframe column names
    array_name: Name of the array in the payload (e.g., "batters", "employees")

    Returns:
    List of payload dictionaries with updated pagination
    """
    payloads = []
    total_chunks = len(chunks)

    # First pass: collect all rows to calculate total records
    all_chunk_data = []
    for chunk in chunks:
        rows = chunk.collect()
        all_chunk_data.append(rows)

    total_records = sum(len(rows) for rows in all_chunk_data)

    # Second pass: build payloads with correct pagination
    for chunk_idx, rows in enumerate(all_chunk_data):
        # Build the array of mapped objects
        mapped_rows = []
        for row in rows:
            mapped_row = map_row_to_structure(row, metadata)
            mapped_rows.append(mapped_row)

        # Create a copy of work request for this payload
        payload_work_request = work_request.copy()

        # Update pagination
        current_page = chunk_idx + 1
        page_size = len(mapped_rows)

        payload_work_request["pagination"] = {
            "currentPage": current_page,
            "pageSize": page_size,
            "totalRecords": total_records,
            "totalPages": total_chunks
        }

        # Create payload
        payload = {
            "workRequest": payload_work_request,
            array_name: mapped_rows
        }

        payloads.append(payload)

        print(f"Page {current_page}/{total_chunks}: {page_size} records")

    return payloads

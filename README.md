# PySpark API Payload Generator

A PySpark-based tool for transforming large datasets into chunked API payloads with metadata-driven field mapping.

## Overview

This project reads data from CSV files using PySpark and generates API payloads in a specific JSON format. It handles large datasets (70k+ rows) by chunking them into manageable pieces and creating properly formatted API requests with pagination.

## Features

- **PySpark Integration**: Efficiently processes large datasets using distributed computing
- **Metadata-Driven Mapping**: Uses metadata dictionaries to map dataframe columns to API field names
- **Automatic Chunking**: Splits large datasets into configurable chunk sizes (default: 1000 rows)
- **Nested Structure Support**: Handles nested JSON structures in the output payload
- **Pagination Management**: Automatically calculates and updates pagination metadata for each chunk
- **Optimized for Fabric**: Designed to run efficiently on Microsoft Fabric's Spark clusters

## Requirements

- Python 3.9+
- PySpark 4.0.1
- Java 17 or 21
- Jupyter Notebook (for development)

## Setup

### 1. Install Java

```bash
brew install openjdk@17
```

Set JAVA_HOME in your shell profile:

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH
```

### 2. Create Virtual Environment

```bash
python -m venv .venv
source .venv/bin/activate  # On macOS/Linux
```

### 3. Install Dependencies

```bash
pip install pyspark jupyter
```

## Usage

See the Jupyter notebook for detailed examples. Basic workflow:

```python
# Initialize Spark
from pyspark_payload_generator import initialize_spark, chunk_dataframe, build_work_request, build_payloads

spark = initialize_spark()

# Read data
df = spark.read.csv("./data/your_data.csv", header=True, inferSchema=True)

# Define metadata mapping
metadata = {
    "field1": "column1",
    "field2": "column2",
    "nestedObject": {
        "nestedField": "column3"
    }
}

# Generate payloads
work_req = build_work_request()
chunks = chunk_dataframe(df, chunk_size=1000)
payloads = build_payloads(work_req, chunks, metadata, "records")
```

## Project Structure

```
.
├── data/                  # Data files (CSV)
├── pyspark_payload_generator.py  # Main module with core functions
├── test_spark.py         # Spark initialization test script
├── .gitignore
└── README.md
```

## Functions

- `initialize_spark()`: Sets up SparkContext and SparkSession
- `chunk_dataframe(df, chunk_size)`: Splits dataframe into chunks
- `build_work_request()`: Creates base work request with GUID and timestamps
- `build_payloads(work_request, chunks, metadata, array_name)`: Generates API payloads with pagination
- `map_row_to_structure(row, metadata)`: Maps dataframe rows to JSON structure

## Testing

This project uses a scaled-down baseball statistics dataset for testing before applying to production datasets.

## License

MIT

## Notes

- Developed for use with Microsoft Fabric
- Optimized for distributed processing using `monotonically_increasing_id()` to avoid performance warnings

# PySpark Payload Generator - Session Summary

**Date:** October 26, 2025
**Project:** PySpark API Payload Generator for Microsoft Fabric

---

## Overview

Today we built a complete PySpark-based solution for transforming large CSV datasets into chunked API payloads with metadata-driven field mapping. This solution will be used in Microsoft Fabric to process ~74,000 row datasets, chunking them into API-ready JSON payloads of 1,000 records each.

---

## Problem Statement

You needed to:
1. Read CSV data into PySpark DataFrames
2. Transform the data into specific JSON API payload structures
3. Chunk large datasets (74k rows) into smaller pieces (1,000 rows each)
4. Use metadata to map dataframe columns to JSON field names
5. Handle nested JSON structures
6. Generate proper pagination metadata for each chunk
7. Test with a scaled-down baseball dataset before production use

---

## Environment Setup

### Initial Challenge: PySpark Installation Issues

**Problem:** PySpark 4.0.1 failed to initialize with `TypeError: 'JavaPackage' object is not callable`

**Root Cause:** Multiple issues:
- Missing Java installation
- Incompatible Java version (initially tried Java 21, then Java 11)
- Incorrect Spark session initialization method

**Solution Steps:**

1. **Installed Java 17** (PySpark 4.0 requires Java 17 or 21, not Java 11)
   ```bash
   brew install openjdk@17
   export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
   ```

2. **Fixed Spark Initialization** - The standard `SparkSession.builder.getOrCreate()` wasn't working. Solution was to create SparkContext first:
   ```python
   from pyspark import SparkContext, SparkConf
   from pyspark.sql import SparkSession

   conf = SparkConf().setAppName("App").setMaster("local[*]")
   sc = SparkContext(conf=conf)
   spark = SparkSession(sc)
   ```

3. **Set Required Environment Variables:**
   ```python
   os.environ['JAVA_HOME'] = '/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home'
   os.environ['SPARK_HOME'] = os.path.join(os.path.dirname(__import__('pyspark').__file__))
   os.environ['PYSPARK_PYTHON'] = 'python3'
   os.environ['PYSPARK_DRIVER_PYTHON'] = 'python3'
   ```

### Key Learnings:
- PySpark 4.0.x requires Java 17 or 21 (Java 11 won't work)
- PySpark 4.0 is very new (2024) and has some initialization quirks
- Must use Python 3.9+ (Python 3.8 support was dropped in Spark 4.0)

---

## Solution Architecture

### Core Components Built

#### 1. **Spark Initialization** (`initialize_spark()`)
- Properly configures SparkContext and SparkSession
- Sets environment variables
- Returns ready-to-use Spark session

#### 2. **DataFrame Chunking** (`chunk_dataframe()`)
- Splits large DataFrames into chunks of specified size (default: 1,000 rows)
- **Initial Implementation Problem:** Used Window functions that caused performance warning:
  ```
  WARN WindowExec: No Partition Defined for Window operation!
  Moving all data to a single partition, this can cause serious performance degradation.
  ```
- **Optimized Solution:** Uses `monotonically_increasing_id()` and `floor()` for distributed processing
  - Leverages cluster parallelization
  - No performance warnings
  - Works efficiently in Microsoft Fabric

#### 3. **Work Request Builder** (`build_work_request()`)
- Generates base API work request structure
- Creates unique GUID for each request
- Formats timestamps correctly:
  - `dataPeriodStartDate`: YYYYMMDD format
  - `extractTimestamp`: ISO format (YYYY-MM-DDThh:mm:ss.sZ)

#### 4. **Metadata-Driven Mapping** (`map_row_to_structure()`)
- Recursively maps DataFrame columns to JSON structure
- Supports nested objects
- Uses metadata dictionary for field mappings

Example metadata:
```python
batters_metadata = {
    "playerID": "playerID",      # Maps to column "playerID"
    "yearID": "yearID",          # Maps to column "yearID"
    "games": "G",                # Maps to column "G"
    "atBats": "AB",              # Maps to column "AB"
    "hits": "H",                 # Maps to column "H"
    "teamInfo": {                # Nested structure
        "league": "lgID"         # Maps to column "lgID"
    }
}
```

#### 5. **Payload Generator** (`build_payloads()`)
- Combines all components
- Generates complete API payloads
- Automatically calculates and updates pagination:
  - `currentPage`: Increments starting at 1
  - `pageSize`: Actual number of records in chunk
  - `totalRecords`: Sum across all chunks
  - `totalPages`: Total number of chunks

**Key Optimization:** Single-pass processing - builds payloads and updates pagination in one enumeration

---

## Output Format

Each generated payload has this structure:

```json
{
  "workRequest": {
    "workRequestId": "529B08935611DEE53E0631A20E70ABA19",
    "type": "BATTERS",
    "mode": "ASYNC",
    "load": "SNAPSHOT",
    "systemCode": "TEST",
    "dataPeriodStartDate": "20251026",
    "extractTimestamp": "2025-10-26T11:24:59.123Z",
    "skipFlag": "N",
    "pagination": {
      "currentPage": 1,
      "pageSize": 1000,
      "totalRecords": 116000,
      "totalPages": 116
    }
  },
  "batters": [
    {
      "playerID": "aaronha01",
      "yearID": "1954",
      "games": "122",
      "atBats": "468",
      "hits": "131",
      "teamInfo": {
        "league": "NL"
      }
    },
    // ... 999 more records ...
  ]
}
```

---

## Test Dataset

Used baseball statistics (Batting.csv) as a scaled-down test case:
- Simulates the structure and complexity of production data
- Allows testing without sensitive data
- Easy to verify results manually

---

## Code Organization

Created a modular, production-ready structure:

```
dictionary_test/
├── .gitignore                      # Python/Jupyter/Spark exclusions
├── README.md                       # Project documentation
├── SESSION_SUMMARY.md              # This file
├── pyspark_payload_generator.py   # Main module with all functions
├── test_spark.py                   # Spark initialization test
├── test_spark2.py                  # Alternative initialization test
└── data/
    ├── Batting.csv                 # Test dataset
    └── Teams.csv                   # Additional test data
```

---

## Functions Reference

### `initialize_spark(app_name="PayloadGenerator")`
Initializes Spark session with proper configuration for PySpark 4.0.1

**Returns:** SparkSession object

---

### `chunk_dataframe(df, chunk_size)`
Splits DataFrame into chunks using distributed processing

**Parameters:**
- `df`: PySpark DataFrame to chunk
- `chunk_size`: Number of rows per chunk (e.g., 1000)

**Returns:** List of PySpark DataFrames

---

### `build_work_request()`
Creates base work request dictionary with GUID and timestamps

**Returns:** Dictionary with work request structure

---

### `map_row_to_structure(row, metadata)`
Recursively maps DataFrame row to JSON structure

**Parameters:**
- `row`: PySpark Row object
- `metadata`: Dictionary mapping JSON keys to column names

**Returns:** Mapped dictionary

---

### `build_payloads(work_request, chunks, metadata, array_name="batters")`
Builds complete API payloads with pagination

**Parameters:**
- `work_request`: Base work request dictionary
- `chunks`: List of PySpark DataFrames
- `metadata`: Field mapping dictionary
- `array_name`: Name of data array in payload (e.g., "batters", "employees")

**Returns:** List of complete payload dictionaries

---

## Usage Example

```python
# Initialize Spark
from pyspark_payload_generator import *

spark = initialize_spark()

# Read data
df = spark.read.csv("./data/Batting.csv", header=True, inferSchema=True)

# Define metadata
batters_metadata = {
    "playerID": "playerID",
    "yearID": "yearID",
    "games": "G",
    "atBats": "AB",
    "hits": "H",
    "teamInfo": {
        "league": "lgID"
    }
}

# Generate payloads
work_req = build_work_request()
chunks = chunk_dataframe(df, chunk_size=1000)
payloads = build_payloads(work_req, chunks, batters_metadata, "batters")

# View results
print(f"Generated {len(payloads)} payloads")
print(f"First payload has {len(payloads[0]['batters'])} records")

# Export as JSON
import json
with open('payload_1.json', 'w') as f:
    json.dump(payloads[0], f, indent=2)
```

---

## Key Technical Decisions

### 1. **Why PySpark 4.0.1?**
Required for Microsoft Fabric compatibility. Fabric uses the latest Spark versions.

### 2. **Why Java 17?**
PySpark 4.0 only supports Java 17 and 21. Java 17 is the most stable LTS version.

### 3. **Why `monotonically_increasing_id()` for chunking?**
- Works efficiently across distributed partitions
- Avoids Window function performance penalties
- Proper for Fabric/cluster environments

### 4. **Why single-pass payload generation?**
- More efficient than separate enumeration for pagination
- Reduces memory overhead
- Cleaner code

### 5. **Why metadata-driven approach?**
- Flexible: Easy to adapt to different payload structures
- Maintainable: Field mappings are declarative, not procedural
- Reusable: Same code works for employees, non-employees, batters, etc.

---

## Performance Considerations

### For Microsoft Fabric Deployment:

1. **Chunking is distributed** - uses Spark's native parallelization
2. **Memory efficient** - processes chunks iteratively, not all at once
3. **Scalable** - tested with 116K rows, ready for 74K production data
4. **No single-partition bottlenecks** - optimized chunking avoids performance warnings

### Expected Performance on 74K Dataset:

- **Chunk size:** 1,000 rows
- **Expected chunks:** 74 payloads
- **Last chunk:** 74,000 % 1,000 = 0 (perfectly divisible) or remainder

---

## Troubleshooting Guide

### Issue: `TypeError: 'JavaPackage' object is not callable`
**Solution:** Use the SparkContext-first initialization method in `initialize_spark()`

### Issue: `Unable to locate a Java Runtime`
**Solution:** Install Java 17 and set JAVA_HOME

### Issue: `ModuleNotFoundError: No module named 'pyspark'`
**Solution:** Install PySpark 4.0.1 in your virtual environment:
```bash
pip install pyspark
```

### Issue: Window function performance warning
**Solution:** Use the optimized `chunk_dataframe()` function with `monotonically_increasing_id()`

### Issue: Jupyter kernel doesn't see JAVA_HOME
**Solution:** Set environment variables directly in notebook before importing PySpark

---

## Next Steps for Production

1. **Update metadata** for your actual production data structure
2. **Update work request defaults** in `build_work_request()`:
   - Change `type` from "BATTERS" to appropriate value
   - Update `systemCode` from "TEST"
   - Adjust other constants as needed
3. **Configure chunk size** - may want different size than 1,000
4. **Add error handling** for API submission
5. **Add logging** for production monitoring
6. **Test with real data** before full deployment

---

## Files Committed to GitHub

- `.gitignore` - Python/Jupyter/Spark exclusions
- `README.md` - Project documentation and setup instructions
- `pyspark_payload_generator.py` - Core module with all functions
- `test_spark.py` - Spark initialization verification script
- `test_spark2.py` - Alternative initialization method
- `data/Batting.csv` - Test dataset
- `data/Teams.csv` - Additional test data

**Repository:** Created and pushed to GitHub (manual setup via Option 2)

---

## Summary

Today we successfully:
✅ Resolved complex PySpark 4.0.1 + Java 17 setup issues on macOS
✅ Built a complete metadata-driven payload generation system
✅ Optimized for distributed processing in Microsoft Fabric
✅ Created modular, reusable, production-ready code
✅ Tested with real-world data structure (baseball stats)
✅ Documented everything for future reference
✅ Set up version control with Git and GitHub

The solution is ready to adapt to your production data and deploy to Microsoft Fabric!

---

## Contact & Support

If you encounter issues:
1. Check the Troubleshooting Guide above
2. Review the README.md for setup steps
3. Verify Java 17 and PySpark 4.0.1 are installed correctly
4. Ensure all environment variables are set

---

**Session completed:** October 26, 2025
**Total development time:** ~2 hours
**Result:** Production-ready PySpark payload generator for Microsoft Fabric

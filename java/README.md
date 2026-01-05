# In-Memory Analytics with Apache Arrow - Java Examples

**This repo is Work In Progress and everything may not work**

Java examples for the book "In-Memory Analytics with Apache Arrow, Second Edition"

## Requirements

- Java 21 or higher
- Maven 3.8+

## Project Structure

This is a multi-module Maven project with the following chapters:

```
├── chapter1/    # Foundations of Apache Arrow
├── chapter2/    # Working with Data Formats (CSV, JSON, ORC, Parquet)
├── chapter4/    # Interoperability and Integration (C Data Interface, GPU)
├── chapter5/    # Compute Functions and Query Execution
├── chapter6/    # Datasets and Streaming
└── chapter8/    # Database Connectivity (ADBC)
```

## Building the Project

```bash
# Build all modules
mvn clean compile

# Build a specific module
mvn clean compile -pl chapter1

# Package all modules
mvn clean package
```

## Running Examples

Each module contains runnable examples. Use the exec-maven-plugin to run them:

```bash
# Chapter 1 - Basic Arrow examples
mvn exec:java -pl chapter1 -Dexec.mainClass="chapter1.Examples"

# Chapter 2 - CSV Reader
mvn exec:java -pl chapter2 -Dexec.mainClass="chapter2.CsvReader"

# Chapter 2 - Parquet Reader/Writer
mvn exec:java -pl chapter2 -Dexec.mainClass="chapter2.ParquetReaderWriter"

# Chapter 4 - C Data Interface
mvn exec:java -pl chapter4 -Dexec.mainClass="chapter4.CDataExample"

# Chapter 5 - Compute Functions
mvn exec:java -pl chapter5 -Dexec.mainClass="chapter5.ComputeFunctions"

# Chapter 5 - Performance Comparison
mvn exec:java -pl chapter5 -Dexec.mainClass="chapter5.ComputeOrNot"

# Chapter 6 - Dataset API
mvn exec:java -pl chapter6 -Dexec.mainClass="chapter6.DatasetsApi"

# Chapter 8 - ADBC SQLite
mvn exec:java -pl chapter8 -Dexec.mainClass="chapter8.AdbcSqlite"

# Chapter 8 - ADBC PostgreSQL
mvn exec:java -pl chapter8 -Dexec.mainClass="chapter8.AdbcPostgres"
```

## Sample Data

Some examples require sample data files. Place them in a `sample_data` directory or update the file paths in the examples.

Required sample files:
- `sample_data/train.csv`
- `sample_data/train.parquet`
- `sample_data/train.orc`
- `sample_data/yellow_tripdata_2015-01.parquet`
- `sample_data/starwars.parquet`

## Module Dependencies

| Module    | Arrow Dependencies                                    |
|-----------|------------------------------------------------------|
| chapter1  | arrow-vector, arrow-memory-netty                     |
| chapter2  | arrow-vector, arrow-memory-netty, arrow-dataset      |
| chapter4  | arrow-vector, arrow-memory-netty, arrow-c-data       |
| chapter5  | arrow-vector, arrow-memory-netty, arrow-dataset      |
| chapter6  | arrow-vector, arrow-memory-netty, arrow-dataset, s3  |
| chapter8  | arrow-vector, arrow-memory-netty, adbc-*             |

## Java Version

This project requires Java 21+ and uses the following modern Java features:
- Virtual threads (preview)
- Pattern matching
- Record patterns
- Enhanced switch expressions

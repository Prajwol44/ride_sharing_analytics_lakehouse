# Databricks notebook source
# DBTITLE 1,Project Overview
# MAGIC %md
# MAGIC # Ride Sharing Analytics Platform - RAW Layer
# MAGIC
# MAGIC ## Project Overview
# MAGIC This notebook implements the **RAW layer** of a multi-layered data lakehouse architecture for a Ride Sharing Analytics Platform.
# MAGIC
# MAGIC ## RAW Layer Purpose
# MAGIC The RAW layer serves as the **initial landing zone** for source data:
# MAGIC - **Ingests** data from source CSV files with minimal transformation
# MAGIC - **Preserves** original data structure and values
# MAGIC - **Adds** metadata for lineage and audit tracking
# MAGIC - **Validates** source data quality (counts, nulls, duplicates)
# MAGIC - **Provides** foundational datasets for downstream processing
# MAGIC
# MAGIC ## Architecture Approach
# MAGIC - **No data cleaning** or standardization at this layer
# MAGIC - **No business logic** or transformations
# MAGIC - **Schema inference** from source files
# MAGIC - **Metadata enrichment** for tracking and lineage
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Import Libraries and Configuration
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

# Configuration - Unity Catalog Volume Path
base_path = "/Volumes/ride_sharing_de_project/raw/raw_data/"

print("✓ Libraries imported successfully")
print(f"✓ Base path: {base_path}")

# COMMAND ----------

# DBTITLE 1,Define Reusable Profiling Function
def profile_dataframe(df, table_name):
    """
    Profile a DataFrame with comprehensive statistics.
    
    Parameters:
    - df: DataFrame to profile
    - table_name: Name of the table for display purposes
    
    Displays:
    - Row count
    - Column count
    - Schema
    - Null count by column
    """
    print("="*80)
    print(f"DATA PROFILE: {table_name}")
    print("="*80)
    
    # Row and column counts
    row_count = df.count()
    col_count = len(df.columns)
    print(f"\n📊 Row Count: {row_count:,}")
    print(f"📊 Column Count: {col_count}")
    
    # Schema
    print("\n📋 Schema:")
    df.printSchema()
    
    # Null counts by column
    print("\n🔍 Null Count by Column:")
    null_counts = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).collect()[0].asDict()
    
    for col_name, null_count in null_counts.items():
        null_pct = (null_count / row_count * 100) if row_count > 0 else 0
        print(f"   {col_name}: {null_count:,} ({null_pct:.2f}%)")
    
    print("\n" + "="*80 + "\n")

print("✓ Profiling function defined: profile_dataframe()")

# COMMAND ----------

# DBTITLE 1,Data Sources
# MAGIC %md
# MAGIC ## Data Sources
# MAGIC
# MAGIC The following CSV files will be ingested into the RAW layer:
# MAGIC
# MAGIC | Source File | Description | Target Variable |
# MAGIC |-------------|-------------|------------------|
# MAGIC | `dim_users.csv` | User dimension data | `raw_users_df` |
# MAGIC | `dim_drivers.csv` | Driver dimension data | `raw_drivers_df` |
# MAGIC | `dim_locations.csv` | Location dimension data | `raw_locations_df` |
# MAGIC | `fact_rides.csv` | Ride transaction facts | `raw_rides_df` |
# MAGIC | `fact_payments.csv` | Payment transaction facts | `raw_payments_df` |
# MAGIC
# MAGIC **Ingestion Strategy:**
# MAGIC - Auto-infer schema from CSV headers
# MAGIC - Add ingestion metadata columns
# MAGIC - Create temporary views for SQL access
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Ingest Users Dimension
# Read dim_users.csv
file_path = base_path + "dim_users.csv"

raw_users_df = spark.read.csv(
    file_path,
    header=True,
    inferSchema=True
)

# Add ingestion metadata
raw_users_df = raw_users_df \
    .withColumn("ingestion_timestamp", F.current_timestamp()) \
    .withColumn("source_file_name", F.lit("dim_users.csv"))

print(f"✓ Loaded: dim_users.csv")
print(f"✓ Row Count: {raw_users_df.count():,}")
print(f"\n📋 Schema:")
raw_users_df.printSchema()

print("\n📊 First 10 Records:")
display(raw_users_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Profile Users Data
profile_dataframe(raw_users_df, "raw_users")

# COMMAND ----------

# DBTITLE 1,Ingest Drivers Dimension
# Read dim_drivers.csv
file_path = base_path + "dim_drivers.csv"

raw_drivers_df = spark.read.csv(
    file_path,
    header=True,
    inferSchema=True
)

# Add ingestion metadata
raw_drivers_df = raw_drivers_df \
    .withColumn("ingestion_timestamp", F.current_timestamp()) \
    .withColumn("source_file_name", F.lit("dim_drivers.csv"))

print(f"✓ Loaded: dim_drivers.csv")
print(f"✓ Row Count: {raw_drivers_df.count():,}")
print(f"\n📋 Schema:")
raw_drivers_df.printSchema()

print("\n📊 First 10 Records:")
display(raw_drivers_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Profile Drivers Data
profile_dataframe(raw_drivers_df, "raw_drivers")

# COMMAND ----------

# DBTITLE 1,Ingest Locations Dimension
# Read dim_locations.csv
file_path = base_path + "dim_locations.csv"

raw_locations_df = spark.read.csv(
    file_path,
    header=True,
    inferSchema=True
)

# Add ingestion metadata
raw_locations_df = raw_locations_df \
    .withColumn("ingestion_timestamp", F.current_timestamp()) \
    .withColumn("source_file_name", F.lit("dim_locations.csv"))

print(f"✓ Loaded: dim_locations.csv")
print(f"✓ Row Count: {raw_locations_df.count():,}")
print(f"\n📋 Schema:")
raw_locations_df.printSchema()

print("\n📊 First 10 Records:")
display(raw_locations_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Profile Locations Data
profile_dataframe(raw_locations_df, "raw_locations")

# COMMAND ----------

# DBTITLE 1,Ingest Rides Fact
# Read fact_rides.csv
file_path = base_path + "fact_rides.csv"

raw_rides_df = spark.read.csv(
    file_path,
    header=True,
    inferSchema=True
)

# Add ingestion metadata
raw_rides_df = raw_rides_df \
    .withColumn("ingestion_timestamp", F.current_timestamp()) \
    .withColumn("source_file_name", F.lit("fact_rides.csv"))

print(f"✓ Loaded: fact_rides.csv")
print(f"✓ Row Count: {raw_rides_df.count():,}")
print(f"\n📋 Schema:")
raw_rides_df.printSchema()

print("\n📊 First 10 Records:")
display(raw_rides_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Profile Rides Data
profile_dataframe(raw_rides_df, "raw_rides")

# COMMAND ----------

# DBTITLE 1,Ingest Payments Fact
# Read fact_payments.csv
file_path = base_path + "fact_payments.csv"

raw_payments_df = spark.read.csv(
    file_path,
    header=True,
    inferSchema=True
)

# Add ingestion metadata
raw_payments_df = raw_payments_df \
    .withColumn("ingestion_timestamp", F.current_timestamp()) \
    .withColumn("source_file_name", F.lit("fact_payments.csv"))

print(f"✓ Loaded: fact_payments.csv")
print(f"✓ Row Count: {raw_payments_df.count():,}")
print(f"\n📋 Schema:")
raw_payments_df.printSchema()

print("\n📊 First 10 Records:")
display(raw_payments_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Profile Payments Data
profile_dataframe(raw_payments_df, "raw_payments")

# COMMAND ----------

# DBTITLE 1,Create Temporary Views
# MAGIC %md
# MAGIC ## Create Temporary Views
# MAGIC
# MAGIC Register all DataFrames as temporary views for SQL-based analysis and downstream processing.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Register Temporary Views
# Create temporary views
raw_users_df.createOrReplaceTempView("raw_users")
raw_drivers_df.createOrReplaceTempView("raw_drivers")
raw_locations_df.createOrReplaceTempView("raw_locations")
raw_rides_df.createOrReplaceTempView("raw_rides")
raw_payments_df.createOrReplaceTempView("raw_payments")

print("✓ Temporary views created:")
print("   - raw_users")
print("   - raw_drivers")
print("   - raw_locations")
print("   - raw_rides")
print("   - raw_payments")

# Verify views
views = spark.sql("SHOW VIEWS").filter(F.col("viewName").startswith("raw_"))
print("\n📋 Registered Views:")
display(views)

# COMMAND ----------

# DBTITLE 1,Source-Level Validation
# MAGIC %md
# MAGIC ## Source-Level Validation
# MAGIC
# MAGIC Perform basic data quality checks on the RAW layer:
# MAGIC * **Total record counts** - Verify data was loaded
# MAGIC * **Duplicate primary keys** - Identify duplicate records
# MAGIC * **Null counts for key columns** - Check for missing critical values
# MAGIC
# MAGIC **Note:** This layer does NOT perform data cleaning or deduplication. Issues identified here will be addressed in downstream layers.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Validation Checks
print("="*80)
print("SOURCE-LEVEL VALIDATION REPORT")
print("="*80)

# Total record counts
print("\n📊 TOTAL RECORD COUNTS")
print("-" * 80)
print(f"raw_users:     {raw_users_df.count():>10,} records")
print(f"raw_drivers:   {raw_drivers_df.count():>10,} records")
print(f"raw_locations: {raw_locations_df.count():>10,} records")
print(f"raw_rides:     {raw_rides_df.count():>10,} records")
print(f"raw_payments:  {raw_payments_df.count():>10,} records")

# Check for duplicate primary keys (assuming first column is PK)
print("\n🔍 DUPLICATE PRIMARY KEY CHECK")
print("-" * 80)

for df_name, df in [("raw_users", raw_users_df), 
                     ("raw_drivers", raw_drivers_df),
                     ("raw_locations", raw_locations_df),
                     ("raw_rides", raw_rides_df),
                     ("raw_payments", raw_payments_df)]:
    
    # Get first column as potential PK
    pk_col = df.columns[0]
    total_records = df.count()
    distinct_records = df.select(pk_col).distinct().count()
    duplicates = total_records - distinct_records
    
    status = "✓ PASS" if duplicates == 0 else "⚠ WARNING"
    print(f"{df_name:20} | PK: {pk_col:15} | Duplicates: {duplicates:>6,} | {status}")

# Check null counts for key columns (first column of each dataset)
print("\n🔍 NULL COUNT CHECK (Primary Key Columns)")
print("-" * 80)

for df_name, df in [("raw_users", raw_users_df), 
                     ("raw_drivers", raw_drivers_df),
                     ("raw_locations", raw_locations_df),
                     ("raw_rides", raw_rides_df),
                     ("raw_payments", raw_payments_df)]:
    
    pk_col = df.columns[0]
    null_count = df.filter(F.col(pk_col).isNull()).count()
    total_count = df.count()
    null_pct = (null_count / total_count * 100) if total_count > 0 else 0
    
    status = "✓ PASS" if null_count == 0 else "⚠ WARNING"
    print(f"{df_name:20} | {pk_col:15} | Nulls: {null_count:>6,} ({null_pct:>5.2f}%) | {status}")

print("\n" + "="*80)
print("✓ SOURCE-LEVEL VALIDATION COMPLETE")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### RAW Layer Ingestion Complete ✓
# MAGIC
# MAGIC This notebook successfully:
# MAGIC * ✅ Ingested 5 source CSV files with schema inference
# MAGIC * ✅ Added ingestion metadata (timestamp, source file name)
# MAGIC * ✅ Created temporary views for SQL access
# MAGIC * ✅ Profiled all datasets with row counts, schemas, and null analysis
# MAGIC * ✅ Validated source data quality (counts, duplicates, nulls)
# MAGIC
# MAGIC ### Available DataFrames:
# MAGIC * `raw_users_df`
# MAGIC * `raw_drivers_df`
# MAGIC * `raw_locations_df`
# MAGIC * `raw_rides_df`
# MAGIC * `raw_payments_df`
# MAGIC
# MAGIC ### Available Temporary Views:
# MAGIC * `raw_users`
# MAGIC * `raw_drivers`
# MAGIC * `raw_locations`
# MAGIC * `raw_rides`
# MAGIC * `raw_payments`

# COMMAND ----------

display(raw_rides_df.limit(10))
display(raw_users_df.limit(10))
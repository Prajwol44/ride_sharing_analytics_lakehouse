# Databricks notebook source
# DBTITLE 1,Bronze Layer Overview
# MAGIC %md
# MAGIC # Ride Sharing Analytics Platform - BRONZE Layer
# MAGIC
# MAGIC ## Bronze Layer Overview
# MAGIC The **BRONZE layer** is the first persistent layer in the Medallion Architecture. It serves as a historical archive of source data in Delta Lake format.
# MAGIC
# MAGIC ## Why Bronze Exists
# MAGIC The Bronze layer provides:
# MAGIC * **Immutable history** - Complete audit trail of all source data
# MAGIC * **Delta Lake benefits** - ACID transactions, time travel, schema evolution
# MAGIC * **Reprocessing capability** - Ability to rebuild downstream layers from scratch
# MAGIC * **Data lineage** - Tracks when data was loaded into the lakehouse
# MAGIC * **Performance** - Optimized storage format vs raw CSV files
# MAGIC
# MAGIC ## Bronze Layer Principles
# MAGIC * **No data cleaning** - Preserve source data exactly as received
# MAGIC * **No deduplication** - Keep all records including duplicates
# MAGIC * **No transformations** - No business logic or standardization
# MAGIC * **Add metadata only** - Track load timestamp and date for lineage
# MAGIC * **Write to Delta** - Store in Delta Lake format for reliability
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Import Libraries and Configuration
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

# Configuration
catalog_name = "ride_sharing_de_project"
schema_name = "bronze"
base_path = "/Volumes/ride_sharing_de_project/raw/raw_data/"

print("✓ Libraries imported successfully")
print(f"✓ Catalog: {catalog_name}")
print(f"✓ Schema: {schema_name}")
print(f"✓ Source path: {base_path}")

# COMMAND ----------

# DBTITLE 1,Create Bronze Schema
# Use the catalog
spark.sql(f"USE CATALOG {catalog_name}")

# Create bronze schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name} COMMENT 'Bronze layer - persistent raw data in Delta format'")

print(f"✓ Using catalog: {catalog_name}")
print(f"✓ Schema '{schema_name}' ready")

# Set default schema
spark.sql(f"USE {schema_name}")
print(f"✓ Default schema set to: {schema_name}")

# COMMAND ----------

# DBTITLE 1,Loading Dimension Tables
# MAGIC %md
# MAGIC ## Loading Dimension Tables
# MAGIC
# MAGIC Dimension tables contain descriptive attributes about business entities:
# MAGIC * **dim_users** - User profiles and segments
# MAGIC * **dim_drivers** - Driver information and ratings
# MAGIC * **dim_locations** - Geographic locations and zones
# MAGIC
# MAGIC These tables are loaded from CSV source files and written to Delta format with bronze metadata.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Load Users Dimension to Bronze
# Read users dimension from source
users_df = spark.read.csv(
    base_path + "dim_users.csv",
    header=True,
    inferSchema=True
)

# Add bronze metadata columns
users_bronze_df = users_df \
    .withColumn("bronze_load_timestamp", F.current_timestamp()) \
    .withColumn("bronze_load_date", F.current_date())

# Write to Delta table
table_name = f"{catalog_name}.{schema_name}.dim_users_bronze"
users_bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(table_name)

print(f"✓ Written: {table_name}")
print(f"✓ Row Count: {users_bronze_df.count():,}")
print("\n📊 First 10 Records:")
display(spark.table(table_name).limit(10))

# COMMAND ----------

# DBTITLE 1,Load Drivers Dimension to Bronze
# Read drivers dimension from source
drivers_df = spark.read.csv(
    base_path + "dim_drivers.csv",
    header=True,
    inferSchema=True
)

# Add bronze metadata columns
drivers_bronze_df = drivers_df \
    .withColumn("bronze_load_timestamp", F.current_timestamp()) \
    .withColumn("bronze_load_date", F.current_date())

# Write to Delta table
table_name = f"{catalog_name}.{schema_name}.dim_drivers_bronze"
drivers_bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(table_name)

print(f"✓ Written: {table_name}")
print(f"✓ Row Count: {drivers_bronze_df.count():,}")
print("\n📊 First 10 Records:")
display(spark.table(table_name).limit(10))

# COMMAND ----------

# DBTITLE 1,Load Locations Dimension to Bronze
# Read locations dimension from source
locations_df = spark.read.csv(
    base_path + "dim_locations.csv",
    header=True,
    inferSchema=True
)

# Add bronze metadata columns
locations_bronze_df = locations_df \
    .withColumn("bronze_load_timestamp", F.current_timestamp()) \
    .withColumn("bronze_load_date", F.current_date())

# Write to Delta table
table_name = f"{catalog_name}.{schema_name}.dim_locations_bronze"
locations_bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(table_name)

print(f"✓ Written: {table_name}")
print(f"✓ Row Count: {locations_bronze_df.count():,}")
print("\n📊 First 10 Records:")
display(spark.table(table_name).limit(10))

# COMMAND ----------

# DBTITLE 1,Loading Fact Tables
# MAGIC %md
# MAGIC ## Loading Fact Tables
# MAGIC
# MAGIC Fact tables contain transactional business events:
# MAGIC * **fact_rides** - Individual ride transactions
# MAGIC * **fact_payments** - Payment transactions
# MAGIC
# MAGIC Fact tables typically have higher volumes than dimensions and are the primary source for analytics.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Load Rides Fact to Bronze
# Read rides fact from source
rides_df = spark.read.csv(
    base_path + "fact_rides.csv",
    header=True,
    inferSchema=True
)

# Add bronze metadata columns
rides_bronze_df = rides_df \
    .withColumn("bronze_load_timestamp", F.current_timestamp()) \
    .withColumn("bronze_load_date", F.current_date())

# Write to Delta table
table_name = f"{catalog_name}.{schema_name}.fact_rides_bronze"
rides_bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(table_name)

print(f"✓ Written: {table_name}")
print(f"✓ Row Count: {rides_bronze_df.count():,}")
print("\n📊 First 10 Records:")
display(spark.table(table_name).limit(10))

# COMMAND ----------

# DBTITLE 1,Load Payments Fact to Bronze
# Read payments fact from source
payments_df = spark.read.csv(
    base_path + "fact_payments.csv",
    header=True,
    inferSchema=True
)

# Add bronze metadata columns
payments_bronze_df = payments_df \
    .withColumn("bronze_load_timestamp", F.current_timestamp()) \
    .withColumn("bronze_load_date", F.current_date())

# Write to Delta table
table_name = f"{catalog_name}.{schema_name}.fact_payments_bronze"
payments_bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(table_name)

print(f"✓ Written: {table_name}")
print(f"✓ Row Count: {payments_bronze_df.count():,}")
print("\n📊 First 10 Records:")
display(spark.table(table_name).limit(10))

# COMMAND ----------

# DBTITLE 1,Audit Logging
# MAGIC %md
# MAGIC ## Audit Logging
# MAGIC
# MAGIC Create a summary table tracking all Bronze layer loads:
# MAGIC * Table name
# MAGIC * Record count
# MAGIC * Load timestamp
# MAGIC
# MAGIC This audit log provides visibility into data pipeline execution and helps troubleshoot issues.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Create Bronze Audit Log
# Create audit log with load statistics
from datetime import datetime

current_time = datetime.now()

audit_data = [
    ("dim_users_bronze", users_bronze_df.count(), current_time),
    ("dim_drivers_bronze", drivers_bronze_df.count(), current_time),
    ("dim_locations_bronze", locations_bronze_df.count(), current_time),
    ("fact_rides_bronze", rides_bronze_df.count(), current_time),
    ("fact_payments_bronze", payments_bronze_df.count(), current_time)
]

# Define schema for audit log
audit_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("record_count", LongType(), False),
    StructField("load_timestamp", TimestampType(), False)
])

# Create audit DataFrame
audit_df = spark.createDataFrame(audit_data, schema=audit_schema)

# Write audit log to Delta table
audit_table_name = f"{catalog_name}.{schema_name}.bronze_audit_log"
audit_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(audit_table_name)

print(f"✓ Created: {audit_table_name}")
print("\n📋 Bronze Layer Audit Log:")
display(spark.table(audit_table_name))

# COMMAND ----------

# DBTITLE 1,Validation
# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC Verify that all Bronze tables were created successfully and contain the expected data.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Validate Bronze Tables
print("="*80)
print("BRONZE LAYER VALIDATION")
print("="*80)

# List all tables in bronze schema
print("\n📋 Tables in Bronze Schema:")
bronze_tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}")
display(bronze_tables)

# Verify record counts match source
print("\n✓ BRONZE LAYER LOAD COMPLETE")
print("-" * 80)
print(f"dim_users_bronze:      {spark.table(f'{catalog_name}.{schema_name}.dim_users_bronze').count():>10,} records")
print(f"dim_drivers_bronze:    {spark.table(f'{catalog_name}.{schema_name}.dim_drivers_bronze').count():>10,} records")
print(f"dim_locations_bronze:  {spark.table(f'{catalog_name}.{schema_name}.dim_locations_bronze').count():>10,} records")
print(f"fact_rides_bronze:     {spark.table(f'{catalog_name}.{schema_name}.fact_rides_bronze').count():>10,} records")
print(f"fact_payments_bronze:  {spark.table(f'{catalog_name}.{schema_name}.fact_payments_bronze').count():>10,} records")
print("-" * 80)
print("\n🎯 All Bronze tables written successfully!")

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### Bronze Layer Complete ✓
# MAGIC
# MAGIC This notebook successfully:
# MAGIC * ✅ Created Bronze schema in Unity Catalog
# MAGIC * ✅ Loaded 5 tables to Delta Lake format
# MAGIC * ✅ Added bronze metadata (load_timestamp, load_date)
# MAGIC * ✅ Preserved source data without modifications
# MAGIC * ✅ Created audit log for tracking
# MAGIC * ✅ Validated all table loads
# MAGIC
# MAGIC ### Bronze Tables Created:
# MAGIC * `ride_sharing_de_project.bronze.dim_users_bronze`
# MAGIC * `ride_sharing_de_project.bronze.dim_drivers_bronze`
# MAGIC * `ride_sharing_de_project.bronze.dim_locations_bronze`
# MAGIC * `ride_sharing_de_project.bronze.fact_rides_bronze`
# MAGIC * `ride_sharing_de_project.bronze.fact_payments_bronze`
# MAGIC * `ride_sharing_de_project.bronze.bronze_audit_log`
# MAGIC
# MAGIC ### Benefits of Bronze Layer:
# MAGIC 1. **Delta Lake ACID guarantees** - Reliable transactions
# MAGIC 2. **Time travel** - Query historical versions
# MAGIC 3. **Schema evolution** - Handle schema changes gracefully
# MAGIC 4. **Optimized storage** - Parquet format with compression
# MAGIC 5. **Audit trail** - Complete lineage tracking
# MAGIC
# MAGIC ### Interview Talking Points:
# MAGIC * Bronze layer preserves raw data without cleaning
# MAGIC * Enables reprocessing of downstream layers
# MAGIC * Provides foundation for data governance
# MAGIC * Delta Lake features improve reliability
# MAGIC * Metadata columns enable lineage tracking
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Next Steps:**
# MAGIC * Build Silver layer with data quality rules
# MAGIC * Apply business transformations
# MAGIC * Create Gold layer for analytics

# COMMAND ----------

display(rides_bronze_df.limit(10))
display(users_bronze_df.limit(10))
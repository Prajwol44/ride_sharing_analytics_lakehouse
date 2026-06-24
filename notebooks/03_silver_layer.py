# Databricks notebook source
# DBTITLE 1,Silver Layer Overview
# MAGIC %md
# MAGIC # Ride Sharing Analytics Platform - SILVER Layer
# MAGIC
# MAGIC ## Silver Layer Overview
# MAGIC The **SILVER layer** is the data quality and transformation layer in the Medallion Architecture. It contains cleaned, validated, and conformed data ready for business analytics.
# MAGIC
# MAGIC ## Purpose of Silver Layer
# MAGIC The Silver layer provides:
# MAGIC * **Data Quality** - Remove duplicates, nulls, and invalid values
# MAGIC * **Standardization** - Consistent formats, naming, and values
# MAGIC * **Basic Transformations** - Date parsing, calculated fields, categorization
# MAGIC * **Business Rules** - Apply domain-specific logic and validation
# MAGIC * **Trusted Data** - Clean foundation for analytics and reporting
# MAGIC
# MAGIC ## Silver Layer Principles
# MAGIC * **Quality over quantity** - Filter out bad data
# MAGIC * **Standardize values** - Consistent formats (uppercase cities, standard categories)
# MAGIC * **Enrich data** - Add derived columns (year, month, flags, buckets)
# MAGIC * **Validate business rules** - Check ranges, required fields
# MAGIC * **Keep it simple** - Use straightforward, maintainable transformations
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Import Libraries and Configuration
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime

# Configuration
catalog_name = "ride_sharing_de_project"
bronze_schema = "bronze"
silver_schema = "silver"

print(f"✓ Catalog: {catalog_name}")
print(f"✓ Bronze Schema: {bronze_schema}")
print(f"✓ Silver Schema: {silver_schema}")

# COMMAND ----------

# DBTITLE 1,Create Silver Schema
# Use the catalog
spark.sql(f"USE CATALOG {catalog_name}")

# Create silver schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver_schema} COMMENT 'Silver layer - cleaned, validated, and conformed data'")

print(f"✓ Using catalog: {catalog_name}")
print(f"✓ Schema '{silver_schema}' ready")

# Set default schema
spark.sql(f"USE {silver_schema}")
print(f"✓ Default schema set to: {silver_schema}")

# COMMAND ----------

# DBTITLE 1,Data Quality Checks
# MAGIC %md
# MAGIC ## Data Quality Checks
# MAGIC
# MAGIC Before transforming data, we need to understand its quality:
# MAGIC * **Record counts** - How much data do we have?
# MAGIC * **Duplicate primary keys** - Are there duplicate IDs?
# MAGIC * **Null values** - Which columns have missing data?
# MAGIC
# MAGIC These checks help us design appropriate cleaning strategies.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Load Bronze Tables
# Load all Bronze tables
users_bronze = spark.table(f"{catalog_name}.{bronze_schema}.dim_users_bronze")
drivers_bronze = spark.table(f"{catalog_name}.{bronze_schema}.dim_drivers_bronze")
locations_bronze = spark.table(f"{catalog_name}.{bronze_schema}.dim_locations_bronze")
rides_bronze = spark.table(f"{catalog_name}.{bronze_schema}.fact_rides_bronze")
payments_bronze = spark.table(f"{catalog_name}.{bronze_schema}.fact_payments_bronze")

print("✓ Bronze tables loaded successfully")
print(f"  - users_bronze: {users_bronze.count():,} records")
print(f"  - drivers_bronze: {drivers_bronze.count():,} records")
print(f"  - locations_bronze: {locations_bronze.count():,} records")
print(f"  - rides_bronze: {rides_bronze.count():,} records")
print(f"  - payments_bronze: {payments_bronze.count():,} records")

# COMMAND ----------

# DBTITLE 1,Data Quality Check - All Tables
print("="*80)
print("DATA QUALITY CHECKS")
print("="*80)

# Define tables and their primary keys
tables = [
    ("users_bronze", users_bronze, "user_id"),
    ("drivers_bronze", drivers_bronze, "driver_id"),
    ("locations_bronze", locations_bronze, "location_id"),
    ("rides_bronze", rides_bronze, "ride_id"),
    ("payments_bronze", payments_bronze, "payment_id")
]

for table_name, df, pk_col in tables:
    print(f"\n{'='*80}")
    print(f"TABLE: {table_name}")
    print(f"{'='*80}")
    
    # Total records
    total_records = df.count()
    print(f"\n📊 Total Records: {total_records:,}")
    
    # Check duplicate primary keys
    distinct_records = df.select(pk_col).distinct().count()
    duplicates = total_records - distinct_records
    status = "✓ PASS" if duplicates == 0 else "⚠️ WARNING"
    print(f"\n🔑 Primary Key: {pk_col}")
    print(f"  Distinct values: {distinct_records:,}")
    print(f"  Duplicates: {duplicates:,} {status}")
    
    # Check null values in ALL columns
    print(f"\n🔍 Null Values by Column:")
    null_counts = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).collect()[0].asDict()
    
    for col_name, null_count in null_counts.items():
        if null_count > 0:
            null_pct = (null_count / total_records * 100)
            print(f"  {col_name}: {null_count:,} ({null_pct:.2f}%) ⚠️")
    
    # If no nulls found
    if all(count == 0 for count in null_counts.values()):
        print(f"  No null values found ✓")

print(f"\n{'='*80}")
print("✓ DATA QUALITY CHECKS COMPLETE")
print(f"{'='*80}")

# COMMAND ----------

# DBTITLE 1,Dimension Transformations
# MAGIC %md
# MAGIC ## Dimension Transformations
# MAGIC
# MAGIC Dimension tables require:
# MAGIC * **Deduplication** - Keep only unique records
# MAGIC * **Standardization** - Consistent formats and values
# MAGIC * **Enrichment** - Add calculated fields
# MAGIC * **Validation** - Ensure data quality
# MAGIC
# MAGIC We'll clean Users, Drivers, and Locations dimensions.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Clean Users Dimension
print("\n" + "="*80)
print("CLEANING USERS DIMENSION")
print("="*80)

# Start with bronze data
users_clean = users_bronze

# Remove duplicate user_id (keep first occurrence)
users_clean = users_clean.dropDuplicates(["user_id"])
print(f"\n✓ Removed duplicates. Records: {users_clean.count():,}")

# Trim whitespace from string columns
string_cols = ["user_id", "user_name", "city", "user_segment", "age_group"]
for col in string_cols:
    users_clean = users_clean.withColumn(col, F.trim(F.col(col)))
print(f"✓ Trimmed whitespace from string columns")

# Standardize city names to uppercase
users_clean = users_clean.withColumn("city", F.upper(F.col("city")))
print(f"✓ Standardized city names to uppercase")

# Standardize user_segment values
users_clean = users_clean.withColumn(
    "user_segment",
    F.initcap(F.col("user_segment"))  # Capitalize first letter
)
print(f"✓ Standardized user_segment values")

# Create user_signup_year from signup_date
users_clean = users_clean.withColumn(
    "user_signup_year",
    F.year(F.col("signup_date"))
)
print(f"✓ Created user_signup_year column")

# Add silver metadata
users_silver = users_clean \
    .withColumn("silver_load_timestamp", F.current_timestamp()) \
    .withColumn("silver_load_date", F.current_date())

# Write to Delta table
table_name = f"{catalog_name}.{silver_schema}.dim_users_silver"
users_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(table_name)

print(f"\n✅ Written: {table_name}")
print(f"✅ Row Count: {users_silver.count():,}")
print(f"\n📋 Schema:")
users_silver.printSchema()
print(f"\n📊 First 10 Records:")
display(spark.table(table_name).limit(10))

# COMMAND ----------

# DBTITLE 1,Clean Drivers Dimension
print("\n" + "="*80)
print("CLEANING DRIVERS DIMENSION")
print("="*80)

# Start with bronze data
drivers_clean = drivers_bronze

# Remove duplicate driver_id
drivers_clean = drivers_clean.dropDuplicates(["driver_id"])
print(f"\n✓ Removed duplicates. Records: {drivers_clean.count():,}")

# Trim string columns
string_cols = ["driver_id", "driver_name", "city", "vehicle_type"]
for col in string_cols:
    drivers_clean = drivers_clean.withColumn(col, F.trim(F.col(col)))
print(f"✓ Trimmed whitespace from string columns")

# Standardize city names to uppercase
drivers_clean = drivers_clean.withColumn("city", F.upper(F.col("city")))
print(f"✓ Standardized city names to uppercase")

# Validate driver_rating between 1 and 5, replace invalid with null
drivers_clean = drivers_clean.withColumn(
    "driver_rating",
    F.when(
        (F.col("driver_rating") >= 1) & (F.col("driver_rating") <= 5),
        F.col("driver_rating")
    ).otherwise(None)
)
print(f"✓ Validated driver_rating (1-5 range)")

# Create driver_experience_band
drivers_clean = drivers_clean.withColumn(
    "driver_experience_band",
    F.when(F.col("years_experience") <= 2, "0-2 Years")
     .when(F.col("years_experience") <= 5, "3-5 Years")
     .when(F.col("years_experience") <= 10, "6-10 Years")
     .otherwise("10+ Years")
)
print(f"✓ Created driver_experience_band column")

# Add silver metadata
drivers_silver = drivers_clean \
    .withColumn("silver_load_timestamp", F.current_timestamp()) \
    .withColumn("silver_load_date", F.current_date())

# Write to Delta table
table_name = f"{catalog_name}.{silver_schema}.dim_drivers_silver"
drivers_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(table_name)

print(f"\n✅ Written: {table_name}")
print(f"✅ Row Count: {drivers_silver.count():,}")
print(f"\n📋 Schema:")
drivers_silver.printSchema()
print(f"\n📊 First 10 Records:")
display(spark.table(table_name).limit(10))

# COMMAND ----------

# DBTITLE 1,Clean Locations Dimension
print("\n" + "="*80)
print("CLEANING LOCATIONS DIMENSION")
print("="*80)

# Start with bronze data
locations_clean = locations_bronze

# Remove duplicates
locations_clean = locations_clean.dropDuplicates(["location_id"])
print(f"\n✓ Removed duplicates. Records: {locations_clean.count():,}")

# Trim string columns
string_cols = ["location_id", "location_name", "zone", "city"]
for col in string_cols:
    locations_clean = locations_clean.withColumn(col, F.trim(F.col(col)))
print(f"✓ Trimmed whitespace from string columns")

# Standardize city names to uppercase
locations_clean = locations_clean.withColumn("city", F.upper(F.col("city")))
print(f"✓ Standardized city names to uppercase")

# Standardize zone names to title case
locations_clean = locations_clean.withColumn("zone", F.initcap(F.col("zone")))
print(f"✓ Standardized zone names to title case")

# Add silver metadata
locations_silver = locations_clean \
    .withColumn("silver_load_timestamp", F.current_timestamp()) \
    .withColumn("silver_load_date", F.current_date())

# Write to Delta table
table_name = f"{catalog_name}.{silver_schema}.dim_locations_silver"
locations_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(table_name)

print(f"\n✅ Written: {table_name}")
print(f"✅ Row Count: {locations_silver.count():,}")
print(f"\n📋 Schema:")
locations_silver.printSchema()
print(f"\n📊 First 10 Records:")
display(spark.table(table_name).limit(10))

# COMMAND ----------

# DBTITLE 1,Fact Transformations
# MAGIC %md
# MAGIC ## Fact Transformations
# MAGIC
# MAGIC Fact tables require:
# MAGIC * **Timestamp conversion** - Parse string timestamps to proper timestamp type
# MAGIC * **Date dimensions** - Extract year, month, day, hour for analysis
# MAGIC * **Business flags** - Create indicators (is_completed, is_cancelled, is_weekend)
# MAGIC * **Calculated fields** - Derive duration buckets, categories
# MAGIC * **Validation** - Filter invalid values (negative amounts, zero distances)
# MAGIC
# MAGIC We'll transform Rides and Payments facts.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Transform Rides Fact
print("\n" + "="*80)
print("TRANSFORMING RIDES FACT")
print("="*80)

# Start with bronze data
rides_transform = rides_bronze

# Convert timestamp columns from string to timestamp (handle "null" strings)
rides_transform = rides_transform \
    .withColumn("booking_timestamp", F.col("booking_timestamp").cast("timestamp")) \
    .withColumn(
        "ride_start_timestamp",
        F.when(F.col("ride_start_timestamp") == "null", None)
         .otherwise(F.col("ride_start_timestamp").cast("timestamp"))
    ) \
    .withColumn(
        "ride_end_timestamp",
        F.when(F.col("ride_end_timestamp") == "null", None)
         .otherwise(F.col("ride_end_timestamp").cast("timestamp"))
    )
print(f"\n✓ Converted timestamp columns")

# Create ride_date from booking_timestamp
rides_transform = rides_transform.withColumn(
    "ride_date",
    F.to_date(F.col("booking_timestamp"))
)
print(f"✓ Created ride_date")

# Create date dimension columns
rides_transform = rides_transform \
    .withColumn("ride_year", F.year(F.col("booking_timestamp"))) \
    .withColumn("ride_month", F.month(F.col("booking_timestamp"))) \
    .withColumn("ride_day", F.dayofmonth(F.col("booking_timestamp"))) \
    .withColumn("ride_hour", F.hour(F.col("booking_timestamp")))
print(f"✓ Created ride_year, ride_month, ride_day, ride_hour")

# Create ride_day_name
rides_transform = rides_transform.withColumn(
    "ride_day_name",
    F.date_format(F.col("booking_timestamp"), "EEEE")
)
print(f"✓ Created ride_day_name")

# Create ride_weekend_flag
rides_transform = rides_transform.withColumn(
    "ride_weekend_flag",
    F.when(F.date_format(F.col("booking_timestamp"), "EEEE").isin(["Saturday", "Sunday"]), "Yes")
     .otherwise("No")
)
print(f"✓ Created ride_weekend_flag")

# Create is_completed flag
rides_transform = rides_transform.withColumn(
    "is_completed",
    F.when(F.col("ride_status") == "Completed", 1).otherwise(0)
)
print(f"✓ Created is_completed flag")

# Create is_cancelled flag
rides_transform = rides_transform.withColumn(
    "is_cancelled",
    F.when(F.col("ride_status") == "Cancelled", 1).otherwise(0)
)
print(f"✓ Created is_cancelled flag")

# Calculate ride duration in minutes (for completed rides only)
rides_transform = rides_transform.withColumn(
    "ride_duration_minutes",
    F.when(
        (F.col("ride_start_timestamp").isNotNull()) & (F.col("ride_end_timestamp").isNotNull()),
        (F.unix_timestamp(F.col("ride_end_timestamp")) - F.unix_timestamp(F.col("ride_start_timestamp"))) / 60
    ).otherwise(None)
)

# Create ride_duration_bucket
rides_transform = rides_transform.withColumn(
    "ride_duration_bucket",
    F.when(F.col("ride_duration_minutes").isNull(), "Unknown")
     .when(F.col("ride_duration_minutes") <= 15, "0-15 mins")
     .when(F.col("ride_duration_minutes") <= 30, "16-30 mins")
     .when(F.col("ride_duration_minutes") <= 60, "31-60 mins")
     .otherwise("60+ mins")
)
print(f"✓ Created ride_duration_bucket")

# Validate distance_km > 0
rides_transform = rides_transform.withColumn(
    "distance_km",
    F.when(F.col("distance_km") > 0, F.col("distance_km")).otherwise(None)
)
print(f"✓ Validated distance_km > 0")

# Validate actual_fare > 0
rides_transform = rides_transform.withColumn(
    "actual_fare",
    F.when(F.col("actual_fare") > 0, F.col("actual_fare")).otherwise(None)
)
print(f"✓ Validated actual_fare > 0")

# Add silver metadata
rides_silver = rides_transform \
    .withColumn("silver_load_timestamp", F.current_timestamp()) \
    .withColumn("silver_load_date", F.current_date())

# Write to Delta table
table_name = f"{catalog_name}.{silver_schema}.fact_rides_silver"
rides_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(table_name)

print(f"\n✅ Written: {table_name}")
print(f"✅ Row Count: {rides_silver.count():,}")
print(f"\n📋 Schema:")
rides_silver.printSchema()
print(f"\n📊 First 10 Records:")
display(spark.table(table_name).limit(10))

# COMMAND ----------

# DBTITLE 1,Transform Payments Fact
print("\n" + "="*80)
print("TRANSFORMING PAYMENTS FACT")
print("="*80)

# Start with bronze data
payments_transform = payments_bronze

# Convert transaction_timestamp to timestamp (handle "null" strings)
payments_transform = payments_transform.withColumn(
    "transaction_timestamp",
    F.when(F.col("transaction_timestamp") == "null", None)
     .otherwise(F.col("transaction_timestamp").cast("timestamp"))
)
print(f"\n✓ Converted transaction_timestamp")

# Create payment_date
payments_transform = payments_transform.withColumn(
    "payment_date",
    F.to_date(F.col("transaction_timestamp"))
)
print(f"✓ Created payment_date")

# Create date dimension columns
payments_transform = payments_transform \
    .withColumn("payment_year", F.year(F.col("transaction_timestamp"))) \
    .withColumn("payment_month", F.month(F.col("transaction_timestamp")))
print(f"✓ Created payment_year, payment_month")

# Validate amount > 0
payments_transform = payments_transform.withColumn(
    "amount",
    F.when(F.col("amount") > 0, F.col("amount")).otherwise(None)
)
print(f"✓ Validated amount > 0")

# Standardize payment_method (title case)
payments_transform = payments_transform.withColumn(
    "payment_method",
    F.initcap(F.trim(F.col("payment_method")))
)
print(f"✓ Standardized payment_method")

# Standardize payment_status (title case)
payments_transform = payments_transform.withColumn(
    "payment_status",
    F.initcap(F.trim(F.col("payment_status")))
)
print(f"✓ Standardized payment_status")

# Add silver metadata
payments_silver = payments_transform \
    .withColumn("silver_load_timestamp", F.current_timestamp()) \
    .withColumn("silver_load_date", F.current_date())

# Write to Delta table
table_name = f"{catalog_name}.{silver_schema}.fact_payments_silver"
payments_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(table_name)

print(f"\n✅ Written: {table_name}")
print(f"✅ Row Count: {payments_silver.count():,}")
print(f"\n📋 Schema:")
payments_silver.printSchema()
print(f"\n📊 First 10 Records:")
display(spark.table(table_name).limit(10))

# COMMAND ----------

# DBTITLE 1,Audit Logging
# MAGIC %md
# MAGIC ## Audit Logging
# MAGIC
# MAGIC Create an audit log to track Silver layer loads:
# MAGIC * Table name
# MAGIC * Record count
# MAGIC * Load timestamp
# MAGIC
# MAGIC This provides visibility into data pipeline execution and helps with troubleshooting.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Create Silver Audit Log
print("\n" + "="*80)
print("CREATING SILVER AUDIT LOG")
print("="*80)

# Create audit log with load statistics
from datetime import datetime

current_time = datetime.now()

audit_data = [
    ("dim_users_silver", users_silver.count(), current_time),
    ("dim_drivers_silver", drivers_silver.count(), current_time),
    ("dim_locations_silver", locations_silver.count(), current_time),
    ("fact_rides_silver", rides_silver.count(), current_time),
    ("fact_payments_silver", payments_silver.count(), current_time)
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
audit_table_name = f"{catalog_name}.{silver_schema}.silver_audit_log"
audit_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(audit_table_name)

print(f"\n✅ Created: {audit_table_name}")
print("\n📋 Silver Layer Audit Log:")
display(spark.table(audit_table_name))

# COMMAND ----------

# DBTITLE 1,Validation Results
# MAGIC %md
# MAGIC ## Validation Results
# MAGIC
# MAGIC Final validation to confirm all Silver tables were created successfully with the expected data quality improvements.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Final Validation
print("\n" + "="*80)
print("SILVER LAYER VALIDATION")
print("="*80)

# List all tables in silver schema
print("\n📋 Tables in Silver Schema:")
silver_tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{silver_schema}")
display(silver_tables)

# Compare Bronze vs Silver record counts
print("\n📊 Record Count Comparison (Bronze → Silver):")
print("-" * 80)

# Users
users_bronze_count = spark.table(f"{catalog_name}.{bronze_schema}.dim_users_bronze").count()
users_silver_count = spark.table(f"{catalog_name}.{silver_schema}.dim_users_silver").count()
print(f"Users:     {users_bronze_count:>10,} → {users_silver_count:>10,} (Removed: {users_bronze_count - users_silver_count:,} duplicates)")

# Drivers
drivers_bronze_count = spark.table(f"{catalog_name}.{bronze_schema}.dim_drivers_bronze").count()
drivers_silver_count = spark.table(f"{catalog_name}.{silver_schema}.dim_drivers_silver").count()
print(f"Drivers:   {drivers_bronze_count:>10,} → {drivers_silver_count:>10,} (Removed: {drivers_bronze_count - drivers_silver_count:,} duplicates)")

# Locations
locations_bronze_count = spark.table(f"{catalog_name}.{bronze_schema}.dim_locations_bronze").count()
locations_silver_count = spark.table(f"{catalog_name}.{silver_schema}.dim_locations_silver").count()
print(f"Locations: {locations_bronze_count:>10,} → {locations_silver_count:>10,} (Removed: {locations_bronze_count - locations_silver_count:,} duplicates)")

# Rides
rides_bronze_count = spark.table(f"{catalog_name}.{bronze_schema}.fact_rides_bronze").count()
rides_silver_count = spark.table(f"{catalog_name}.{silver_schema}.fact_rides_silver").count()
print(f"Rides:     {rides_bronze_count:>10,} → {rides_silver_count:>10,}")

# Payments
payments_bronze_count = spark.table(f"{catalog_name}.{bronze_schema}.fact_payments_bronze").count()
payments_silver_count = spark.table(f"{catalog_name}.{silver_schema}.fact_payments_silver").count()
print(f"Payments:  {payments_bronze_count:>10,} → {payments_silver_count:>10,}")

print("-" * 80)
print("\n✅ SILVER LAYER VALIDATION COMPLETE")
print("\n🎯 All Silver tables written successfully!")
print("\n📌 Next Step: Create Gold layer with business aggregations and metrics")

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### Silver Layer Complete ✓
# MAGIC
# MAGIC This notebook successfully:
# MAGIC * ✅ Performed comprehensive data quality checks on all Bronze tables
# MAGIC * ✅ Cleaned and standardized 3 dimension tables
# MAGIC * ✅ Transformed and enriched 2 fact tables
# MAGIC * ✅ Removed duplicates from all dimension tables
# MAGIC * ✅ Validated data ranges (ratings, amounts, distances)
# MAGIC * ✅ Created derived columns (flags, buckets, date dimensions)
# MAGIC * ✅ Standardized values (cities, categories, payment methods)
# MAGIC * ✅ Created audit log for tracking
# MAGIC
# MAGIC ### Silver Tables Created:
# MAGIC * `ride_sharing_de_project.silver.dim_users_silver`
# MAGIC * `ride_sharing_de_project.silver.dim_drivers_silver`
# MAGIC * `ride_sharing_de_project.silver.dim_locations_silver`
# MAGIC * `ride_sharing_de_project.silver.fact_rides_silver`
# MAGIC * `ride_sharing_de_project.silver.fact_payments_silver`
# MAGIC * `ride_sharing_de_project.silver.silver_audit_log`
# MAGIC
# MAGIC ### Key Transformations Applied:
# MAGIC
# MAGIC **Dimension Tables:**
# MAGIC * Deduplication by primary key
# MAGIC * String trimming and standardization
# MAGIC * City names uppercase
# MAGIC * Derived fields (signup_year, experience_band)
# MAGIC * Rating validation (1-5 range)
# MAGIC
# MAGIC **Fact Tables:**
# MAGIC * Timestamp conversion from strings
# MAGIC * Date dimension creation (year, month, day, hour)
# MAGIC * Business flags (is_completed, is_cancelled, weekend_flag)
# MAGIC * Duration buckets (0-15, 16-30, 31-60, 60+ mins)
# MAGIC * Data validation (positive amounts, distances)
# MAGIC * Payment method/status standardization
# MAGIC
# MAGIC ### Interview Talking Points:
# MAGIC * Silver layer ensures data quality before analytics
# MAGIC * Simple, maintainable transformations (no complex window functions)
# MAGIC * Validation rules enforce business constraints
# MAGIC * Derived columns enable easier analysis
# MAGIC * Standardization ensures consistency across reports
# MAGIC * Audit logging provides pipeline visibility
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Ready for Gold Layer:**
# MAGIC The Silver layer now provides clean, validated, business-ready data for aggregations and analytics!

# COMMAND ----------

display(rides_silver.limit(10))
display(users_silver.limit(10))
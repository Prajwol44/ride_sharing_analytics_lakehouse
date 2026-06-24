# Databricks notebook source
# DBTITLE 1,Gold Layer Overview
# MAGIC %md
# MAGIC # Ride Sharing Analytics Platform - GOLD Layer
# MAGIC
# MAGIC ## Gold Layer Overview
# MAGIC The **GOLD layer** is the business-ready analytics layer in the Medallion Architecture. It contains aggregated, denormalized tables optimized for reporting, dashboards, and business intelligence.
# MAGIC
# MAGIC ## Purpose of Gold Layer
# MAGIC The Gold layer provides:
# MAGIC * **Business Metrics** - Pre-calculated KPIs and aggregations
# MAGIC * **Analytical Tables** - Ready for dashboards and BI tools
# MAGIC * **Performance** - Optimized for query speed
# MAGIC * **Business Logic** - Domain-specific calculations and insights
# MAGIC * **Accessibility** - Easy-to-understand tables for business users
# MAGIC
# MAGIC ## Gold Layer Tables
# MAGIC 1. **Revenue Analytics** - Overall revenue summary
# MAGIC 2. **City Revenue** - Revenue breakdown by city
# MAGIC 3. **Driver Performance** - Driver metrics and ratings
# MAGIC 4. **Customer Analytics** - Customer behavior and spending
# MAGIC 5. **Demand Analysis** - Hourly ride demand patterns
# MAGIC 6. **Cancellation Analysis** - Cancellation reasons breakdown
# MAGIC 7. **Vehicle Performance** - Vehicle type analytics
# MAGIC 8. **Executive Dashboard** - Single-row KPI summary
# MAGIC
# MAGIC ## Gold Layer Principles
# MAGIC * **Aggregated data** - Pre-calculated metrics for fast queries
# MAGIC * **Denormalized** - Joins already done for easy access
# MAGIC * **Business-focused** - Metrics that matter to stakeholders
# MAGIC * **Dashboard-ready** - Optimized for visualization tools
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Import Libraries and Configuration
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

# Configuration
catalog_name = "ride_sharing_de_project"
silver_schema = "silver"
gold_schema = "gold"

print("✓ Libraries imported successfully")
print(f"✓ Catalog: {catalog_name}")
print(f"✓ Silver Schema: {silver_schema}")
print(f"✓ Gold Schema: {gold_schema}")

# COMMAND ----------

# DBTITLE 1,Create Gold Schema
# Use the catalog
spark.sql(f"USE CATALOG {catalog_name}")

# Create gold schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_schema} COMMENT 'Gold layer - business-ready analytical tables'")

print(f"✓ Using catalog: {catalog_name}")
print(f"✓ Schema '{gold_schema}' ready")

# Set default schema
spark.sql(f"USE {gold_schema}")
print(f"✓ Default schema set to: {gold_schema}")

# COMMAND ----------

# DBTITLE 1,Load Silver Tables
# Load all Silver tables
users_silver = spark.table(f"{catalog_name}.{silver_schema}.dim_users_silver")
drivers_silver = spark.table(f"{catalog_name}.{silver_schema}.dim_drivers_silver")
locations_silver = spark.table(f"{catalog_name}.{silver_schema}.dim_locations_silver")
rides_silver = spark.table(f"{catalog_name}.{silver_schema}.fact_rides_silver")
payments_silver = spark.table(f"{catalog_name}.{silver_schema}.fact_payments_silver")

print("✓ Silver tables loaded successfully")
print(f"  - users_silver: {users_silver.count():,} records")
print(f"  - drivers_silver: {drivers_silver.count():,} records")
print(f"  - locations_silver: {locations_silver.count():,} records")
print(f"  - rides_silver: {rides_silver.count():,} records")
print(f"  - payments_silver: {payments_silver.count():,} records")

# COMMAND ----------

# DBTITLE 1,Revenue Analytics
# MAGIC %md
# MAGIC ## Revenue Analytics
# MAGIC
# MAGIC Overall revenue summary with key business metrics:
# MAGIC * Total rides and completion status
# MAGIC * Revenue totals and averages
# MAGIC * Distance metrics
# MAGIC
# MAGIC This is the primary KPI table for executive reporting.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Create Revenue Summary Table
print("\n" + "="*80)
print("CREATING REVENUE SUMMARY")
print("="*80)

# Calculate overall revenue metrics
revenue_summary = rides_silver.agg(
    F.count("*").alias("total_rides"),
    F.sum("is_completed").alias("completed_rides"),
    F.sum("is_cancelled").alias("cancelled_rides"),
    F.sum(F.when(F.col("is_completed") == 1, F.col("actual_fare")).otherwise(0)).alias("total_revenue"),
    F.avg(F.when(F.col("is_completed") == 1, F.col("actual_fare")).otherwise(None)).alias("average_fare"),
    F.avg(F.when(F.col("is_completed") == 1, F.col("distance_km")).otherwise(None)).alias("average_distance")
)

# Write to Delta table
table_name = f"{catalog_name}.{gold_schema}.gold_revenue_summary"
revenue_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(table_name)

print(f"\n✅ Written: {table_name}")
print(f"✅ Row Count: {revenue_summary.count():,}")
print(f"\n📊 Revenue Summary:")
display(spark.table(table_name))

# COMMAND ----------

# DBTITLE 1,Revenue By City
# MAGIC %md
# MAGIC ## Revenue By City
# MAGIC
# MAGIC City-wise revenue breakdown by joining rides with pickup locations:
# MAGIC * Total and completed rides per city
# MAGIC * Revenue and average fare by city
# MAGIC * Sorted by revenue (highest first)
# MAGIC
# MAGIC Useful for identifying top-performing markets.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Create City Revenue Table
print("\n" + "="*80)
print("CREATING CITY REVENUE ANALYSIS")
print("="*80)

# Join rides with pickup locations to get city
city_revenue = rides_silver.alias("r") \
    .join(
        locations_silver.alias("l"),
        F.col("r.pickup_location_id") == F.col("l.location_id"),
        "left"
    ) \
    .groupBy(F.col("l.city").alias("city")) \
    .agg(
        F.count("*").alias("total_rides"),
        F.sum("r.is_completed").alias("completed_rides"),
        F.sum(F.when(F.col("r.is_completed") == 1, F.col("r.actual_fare")).otherwise(0)).alias("revenue"),
        F.avg(F.when(F.col("r.is_completed") == 1, F.col("r.actual_fare")).otherwise(None)).alias("average_fare")
    ) \
    .orderBy(F.col("revenue").desc())

# Write to Delta table
table_name = f"{catalog_name}.{gold_schema}.gold_city_revenue"
city_revenue.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(table_name)

print(f"\n✅ Written: {table_name}")
print(f"✅ Row Count: {city_revenue.count():,}")
print(f"\n📊 Top Cities by Revenue:")
display(spark.table(table_name).limit(10))

# COMMAND ----------

# DBTITLE 1,Driver Analytics
# MAGIC %md
# MAGIC ## Driver Performance Analytics
# MAGIC
# MAGIC Driver-level performance metrics:
# MAGIC * Ride completion and cancellation rates
# MAGIC * Customer ratings
# MAGIC * Revenue generated per driver
# MAGIC * Vehicle type used
# MAGIC
# MAGIC Essential for driver management and incentive programs.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Create Driver Performance Table
print("\n" + "="*80)
print("CREATING DRIVER PERFORMANCE ANALYTICS")
print("="*80)

# Join rides with drivers
driver_performance = rides_silver.alias("r") \
    .join(
        drivers_silver.alias("d"),
        F.col("r.driver_id") == F.col("d.driver_id"),
        "left"
    ) \
    .groupBy(
        F.col("r.driver_id").alias("driver_id"),
        F.col("d.vehicle_type").alias("vehicle_type")
    ) \
    .agg(
        F.sum("r.is_completed").alias("completed_rides"),
        F.sum("r.is_cancelled").alias("cancelled_rides"),
        F.count("*").alias("total_rides")
    ) \
    .withColumn(
        "completion_rate",
        F.round((F.col("completed_rides") / F.col("total_rides")) * 100, 2)
    )

# Add average customer rating and revenue
driver_performance = driver_performance.alias("dp") \
    .join(
        rides_silver.filter(F.col("is_completed") == 1).alias("r"),
        F.col("dp.driver_id") == F.col("r.driver_id"),
        "left"
    ) \
    .groupBy(
        F.col("dp.driver_id"),
        F.col("dp.vehicle_type"),
        F.col("dp.completed_rides"),
        F.col("dp.cancelled_rides"),
        F.col("dp.completion_rate")
    ) \
    .agg(
        F.avg(F.col("r.customer_rating").cast("double")).alias("average_customer_rating"),
        F.sum(F.col("r.actual_fare")).alias("revenue_generated")
    ) \
    .orderBy(F.col("revenue_generated").desc())

# Write to Delta table
table_name = f"{catalog_name}.{gold_schema}.gold_driver_performance"
driver_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(table_name)

print(f"\n✅ Written: {table_name}")
print(f"✅ Row Count: {driver_performance.count():,}")
print(f"\n📊 Top Drivers by Revenue:")
display(spark.table(table_name).limit(10))

# COMMAND ----------

# DBTITLE 1,Customer Analytics
# MAGIC %md
# MAGIC ## Customer Analytics
# MAGIC
# MAGIC Customer-level behavior and spending patterns:
# MAGIC * Ride frequency and completion status
# MAGIC * Total spending and average fare
# MAGIC * Customer lifetime value indicators
# MAGIC
# MAGIC Key for customer segmentation and retention strategies.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Create Customer Analytics Table
print("\n" + "="*80)
print("CREATING CUSTOMER ANALYTICS")
print("="*80)

# Aggregate customer metrics from rides
customer_analytics = rides_silver.groupBy("user_id") \
    .agg(
        F.count("*").alias("total_rides"),
        F.sum("is_completed").alias("completed_rides"),
        F.sum("is_cancelled").alias("cancelled_rides"),
        F.sum(F.when(F.col("is_completed") == 1, F.col("actual_fare")).otherwise(0)).alias("total_spending"),
        F.avg(F.when(F.col("is_completed") == 1, F.col("actual_fare")).otherwise(None)).alias("average_ride_fare")
    ) \
    .orderBy(F.col("total_spending").desc())

# Write to Delta table
table_name = f"{catalog_name}.{gold_schema}.gold_customer_analytics"
customer_analytics.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(table_name)

print(f"\n✅ Written: {table_name}")
print(f"✅ Row Count: {customer_analytics.count():,}")
print(f"\n📊 Top Customers by Spending:")
display(spark.table(table_name).limit(10))

# COMMAND ----------

# DBTITLE 1,Demand Analytics
# MAGIC %md
# MAGIC ## Demand Analysis
# MAGIC
# MAGIC Hourly demand patterns:
# MAGIC * Ride volume by hour of day
# MAGIC * Completion vs cancellation by time
# MAGIC * Revenue by hour
# MAGIC
# MAGIC Critical for driver allocation and surge pricing strategies.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Create Demand By Hour Table
print("\n" + "="*80)
print("CREATING DEMAND BY HOUR ANALYSIS")
print("="*80)

# Aggregate demand by hour
demand_by_hour = rides_silver.groupBy("ride_hour") \
    .agg(
        F.count("*").alias("ride_count"),
        F.sum("is_completed").alias("completed_rides"),
        F.sum("is_cancelled").alias("cancelled_rides"),
        F.sum(F.when(F.col("is_completed") == 1, F.col("actual_fare")).otherwise(0)).alias("revenue")
    ) \
    .orderBy("ride_hour")

# Write to Delta table
table_name = f"{catalog_name}.{gold_schema}.gold_demand_by_hour"
demand_by_hour.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(table_name)

print(f"\n✅ Written: {table_name}")
print(f"✅ Row Count: {demand_by_hour.count():,}")
print(f"\n📊 Hourly Demand Pattern:")
display(spark.table(table_name))

# COMMAND ----------

# DBTITLE 1,Cancellation Analytics
# MAGIC %md
# MAGIC ## Cancellation Analysis
# MAGIC
# MAGIC Cancellation reasons breakdown:
# MAGIC * Count of cancellations by reason
# MAGIC * Percentage of total cancellations
# MAGIC
# MAGIC Helps identify operational issues and areas for improvement.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Create Cancellation Analysis Table
print("\n" + "="*80)
print("CREATING CANCELLATION ANALYSIS")
print("="*80)

# Aggregate cancellation reasons
total_cancellations = rides_silver.filter(F.col("is_cancelled") == 1).count()

cancellation_analysis = rides_silver \
    .filter(F.col("is_cancelled") == 1) \
    .groupBy("cancellation_reason") \
    .agg(
        F.count("*").alias("cancelled_rides")
    ) \
    .withColumn(
        "percentage_of_total_cancellations",
        F.round((F.col("cancelled_rides") / F.lit(total_cancellations)) * 100, 2)
    ) \
    .orderBy(F.col("cancelled_rides").desc())

# Write to Delta table
table_name = f"{catalog_name}.{gold_schema}.gold_cancellation_analysis"
cancellation_analysis.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(table_name)

print(f"\n✅ Written: {table_name}")
print(f"✅ Row Count: {cancellation_analysis.count():,}")
print(f"\n📊 Cancellation Reasons:")
display(spark.table(table_name))

# COMMAND ----------

# DBTITLE 1,Vehicle Analytics
# MAGIC %md
# MAGIC ## Vehicle Performance Analytics
# MAGIC
# MAGIC Vehicle type performance metrics:
# MAGIC * Ride volume by vehicle type
# MAGIC * Revenue and average fare
# MAGIC * Customer ratings by vehicle type
# MAGIC
# MAGIC Informs fleet composition and vehicle allocation decisions.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Create Vehicle Performance Table
print("\n" + "="*80)
print("CREATING VEHICLE PERFORMANCE ANALYTICS")
print("="*80)

# Join rides with drivers to get vehicle type
vehicle_performance = rides_silver.alias("r") \
    .join(
        drivers_silver.alias("d"),
        F.col("r.driver_id") == F.col("d.driver_id"),
        "left"
    ) \
    .filter(F.col("r.is_completed") == 1) \
    .groupBy(F.col("d.vehicle_type").alias("vehicle_type")) \
    .agg(
        F.count("*").alias("ride_count"),
        F.sum(F.col("r.actual_fare")).alias("revenue"),
        F.avg(F.col("r.actual_fare")).alias("average_fare"),
        F.avg(F.col("r.customer_rating").cast("double")).alias("average_rating")
    ) \
    .orderBy(F.col("revenue").desc())

# Write to Delta table
table_name = f"{catalog_name}.{gold_schema}.gold_vehicle_performance"
vehicle_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(table_name)

print(f"\n✅ Written: {table_name}")
print(f"✅ Row Count: {vehicle_performance.count():,}")
print(f"\n📊 Vehicle Performance:")
display(spark.table(table_name))

# COMMAND ----------

# DBTITLE 1,Executive Dashboard
# MAGIC %md
# MAGIC ## Executive Dashboard
# MAGIC
# MAGIC Single-row summary with all critical KPIs:
# MAGIC * User, driver, and ride totals
# MAGIC * Completion and cancellation metrics
# MAGIC * Revenue and fare averages
# MAGIC
# MAGIC Perfect for executive dashboards and high-level reporting.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Create Executive Dashboard Table
print("\n" + "="*80)
print("CREATING EXECUTIVE DASHBOARD")
print("="*80)

# Calculate executive KPIs
total_users = users_silver.count()
total_drivers = drivers_silver.count()
total_rides = rides_silver.count()
completed_rides = rides_silver.filter(F.col("is_completed") == 1).count()
cancelled_rides = rides_silver.filter(F.col("is_cancelled") == 1).count()
cancellation_rate = round((cancelled_rides / total_rides) * 100, 2)

revenue_metrics = rides_silver.filter(F.col("is_completed") == 1).agg(
    F.sum("actual_fare").alias("total_revenue"),
    F.avg("actual_fare").alias("avg_fare"),
    F.avg("distance_km").alias("avg_distance")
).collect()[0]

# Create single-row DataFrame
executive_dashboard = spark.createDataFrame([
    (
        total_users,
        total_drivers,
        total_rides,
        completed_rides,
        cancelled_rides,
        cancellation_rate,
        revenue_metrics["total_revenue"],
        revenue_metrics["avg_fare"],
        revenue_metrics["avg_distance"]
    )
], [
    "total_users",
    "total_drivers",
    "total_rides",
    "completed_rides",
    "cancelled_rides",
    "cancellation_rate_pct",
    "total_revenue",
    "avg_fare",
    "avg_distance"
])

# Write to Delta table
table_name = f"{catalog_name}.{gold_schema}.gold_executive_dashboard"
executive_dashboard.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(table_name)

print(f"\n✅ Written: {table_name}")
print(f"✅ Row Count: {executive_dashboard.count():,}")
print(f"\n📊 Executive Dashboard:")
display(spark.table(table_name))

# COMMAND ----------

# DBTITLE 1,Audit Logging
# MAGIC %md
# MAGIC ## Audit Logging
# MAGIC
# MAGIC Create an audit log to track Gold layer loads:
# MAGIC * Table name
# MAGIC * Record count
# MAGIC * Load timestamp
# MAGIC
# MAGIC Provides visibility into data pipeline execution.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Create Gold Audit Log
print("\n" + "="*80)
print("CREATING GOLD AUDIT LOG")
print("="*80)

# Create audit log with load statistics
from datetime import datetime

current_time = datetime.now()

audit_data = [
    ("gold_revenue_summary", spark.table(f"{catalog_name}.{gold_schema}.gold_revenue_summary").count(), current_time),
    ("gold_city_revenue", spark.table(f"{catalog_name}.{gold_schema}.gold_city_revenue").count(), current_time),
    ("gold_driver_performance", spark.table(f"{catalog_name}.{gold_schema}.gold_driver_performance").count(), current_time),
    ("gold_customer_analytics", spark.table(f"{catalog_name}.{gold_schema}.gold_customer_analytics").count(), current_time),
    ("gold_demand_by_hour", spark.table(f"{catalog_name}.{gold_schema}.gold_demand_by_hour").count(), current_time),
    ("gold_cancellation_analysis", spark.table(f"{catalog_name}.{gold_schema}.gold_cancellation_analysis").count(), current_time),
    ("gold_vehicle_performance", spark.table(f"{catalog_name}.{gold_schema}.gold_vehicle_performance").count(), current_time),
    ("gold_executive_dashboard", spark.table(f"{catalog_name}.{gold_schema}.gold_executive_dashboard").count(), current_time)
]

# Define schema for audit log
audit_schema_struct = StructType([
    StructField("table_name", StringType(), False),
    StructField("record_count", LongType(), False),
    StructField("load_timestamp", TimestampType(), False)
])

# Create audit DataFrame
audit_df = spark.createDataFrame(audit_data, schema=audit_schema_struct)

# Write audit log to Delta table
audit_table_name = f"{catalog_name}.{gold_schema}.gold_audit_log"
audit_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(audit_table_name)

print(f"\n✅ Created: {audit_table_name}")
print("\n📋 Gold Layer Audit Log:")
display(spark.table(audit_table_name))

# COMMAND ----------

# DBTITLE 1,Validation
# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC Final validation to confirm all Gold tables were created successfully.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Final Validation
print("\n" + "="*80)
print("GOLD LAYER VALIDATION")
print("="*80)

# List all tables in gold schema
print("\n📋 Tables in Gold Schema:")
gold_tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{gold_schema}")
display(gold_tables)

# Summary of all gold tables
print("\n📊 Gold Layer Summary:")
print("-" * 80)
print(f"gold_revenue_summary:          {spark.table(f'{catalog_name}.{gold_schema}.gold_revenue_summary').count():>10,} records")
print(f"gold_city_revenue:             {spark.table(f'{catalog_name}.{gold_schema}.gold_city_revenue').count():>10,} records")
print(f"gold_driver_performance:       {spark.table(f'{catalog_name}.{gold_schema}.gold_driver_performance').count():>10,} records")
print(f"gold_customer_analytics:       {spark.table(f'{catalog_name}.{gold_schema}.gold_customer_analytics').count():>10,} records")
print(f"gold_demand_by_hour:           {spark.table(f'{catalog_name}.{gold_schema}.gold_demand_by_hour').count():>10,} records")
print(f"gold_cancellation_analysis:    {spark.table(f'{catalog_name}.{gold_schema}.gold_cancellation_analysis').count():>10,} records")
print(f"gold_vehicle_performance:      {spark.table(f'{catalog_name}.{gold_schema}.gold_vehicle_performance').count():>10,} records")
print(f"gold_executive_dashboard:      {spark.table(f'{catalog_name}.{gold_schema}.gold_executive_dashboard').count():>10,} records")
print("-" * 80)
print("\n✅ GOLD LAYER VALIDATION COMPLETE")
print("\n🎯 All Gold tables created successfully!")
print("\n📌 Ready for Business Intelligence and Analytics!")

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### Gold Layer Complete ✓
# MAGIC
# MAGIC This notebook successfully:
# MAGIC * ✅ Created Gold schema in Unity Catalog
# MAGIC * ✅ Built 8 analytical tables from Silver layer data
# MAGIC * ✅ Performed aggregations and joins
# MAGIC * ✅ Created business-ready metrics and KPIs
# MAGIC * ✅ Generated audit log for tracking
# MAGIC * ✅ Validated all table loads
# MAGIC
# MAGIC ### Gold Tables Created:
# MAGIC * `gold_revenue_summary` - Overall revenue and ride metrics
# MAGIC * `gold_city_revenue` - City-wise revenue breakdown
# MAGIC * `gold_driver_performance` - Driver analytics and ratings
# MAGIC * `gold_customer_analytics` - Customer behavior and spending
# MAGIC * `gold_demand_by_hour` - Hourly demand patterns
# MAGIC * `gold_cancellation_analysis` - Cancellation reasons breakdown
# MAGIC * `gold_vehicle_performance` - Vehicle type analytics
# MAGIC * `gold_executive_dashboard` - Single-row KPI summary
# MAGIC * `gold_audit_log` - Pipeline tracking
# MAGIC
# MAGIC ### Key Analytical Capabilities:
# MAGIC
# MAGIC **Revenue Analysis:**
# MAGIC * Total and average revenue metrics
# MAGIC * City-level revenue comparison
# MAGIC * Vehicle type profitability
# MAGIC
# MAGIC **Operational Insights:**
# MAGIC * Completion and cancellation rates
# MAGIC * Hourly demand patterns for resource planning
# MAGIC * Cancellation reason analysis
# MAGIC
# MAGIC **Performance Metrics:**
# MAGIC * Driver performance and ratings
# MAGIC * Vehicle type effectiveness
# MAGIC * Customer lifetime value indicators
# MAGIC
# MAGIC **Executive Reporting:**
# MAGIC * Single-row dashboard with all KPIs
# MAGIC * Ready for BI tools and visualization
# MAGIC * Pre-aggregated for fast query performance
# MAGIC
# MAGIC ### Interview Talking Points:
# MAGIC * Gold layer contains business-ready analytics
# MAGIC * Aggregations done once, queried many times
# MAGIC * Denormalized for performance and simplicity
# MAGIC * Clear separation between operational (Silver) and analytical (Gold) data
# MAGIC * Designed for business users and dashboards
# MAGIC * Easy to understand and explain
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **🎉 Medallion Architecture Complete!**
# MAGIC
# MAGIC **RAW → BRONZE → SILVER → GOLD**
# MAGIC
# MAGIC The end-to-end data pipeline is now ready for production use!
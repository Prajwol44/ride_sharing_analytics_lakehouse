# 🚖 Ride Sharing Analytics Lakehouse

An end-to-end Data Engineering project built using **Databricks, PySpark, Delta Lake, SQL, and Unity Catalog** that simulates a real-world ride-sharing analytics platform. The project processes ride, payment, customer, driver, and location data through a Medallion Architecture (RAW → BRONZE → SILVER → GOLD) to transform raw operational data into business-ready analytical datasets.

---

# 📌 Project Overview

Ride-sharing platforms generate large volumes of operational data every day. Business teams need reliable insights into:

- Revenue performance
- Driver efficiency
- Customer behavior
- Demand patterns
- Ride cancellations
- Vehicle utilization

Raw transactional data is difficult to analyze directly. This project demonstrates how modern data engineering practices can be used to build a scalable analytics platform that transforms source data into curated business intelligence tables.

---

# 🛠 Tech Stack

| Category | Technology |
|----------|------------|
| Data Platform | Databricks |
| Processing Engine | PySpark |
| Storage Layer | Delta Lake |
| Catalog & Governance | Unity Catalog |
| Query Engine | Spark SQL |
| Architecture | Medallion Architecture |
| Language | Python |

---

# 🏗 Architecture

The platform follows a Medallion Architecture pattern to progressively improve data quality and business value.

![Architecture](docs/architecture.png)

### Flow

```text
Source Files
      ↓
RAW Layer
      ↓
BRONZE Layer
      ↓
SILVER Layer
      ↓
GOLD Layer
      ↓
Business Insights
```

---

# 📂 Source Datasets

The project ingests five datasets representing a ride-sharing ecosystem.

### Dimension Tables

| Dataset | Description |
|----------|-------------|
| dim_users.csv | Customer information |
| dim_drivers.csv | Driver information |
| dim_locations.csv | Location metadata |

### Fact Tables

| Dataset | Description |
|----------|-------------|
| fact_rides.csv | Ride transactions |
| fact_payments.csv | Payment transactions |

---

# 🔹 RAW Layer

### Objective

Preserve source data exactly as received.

### Activities Performed

- CSV ingestion
- Schema inference
- Data profiling
- Metadata capture
- Record count validation
- Null value analysis

### Output

Raw DataFrames created for:

```text
raw_users_df
raw_drivers_df
raw_locations_df
raw_rides_df
raw_payments_df
```

### Screenshots

#### Unity Catalog Structure

![Catalog](screenshots/00_catalog.png)

#### Raw Layer Data

![Raw Dataset](screenshots/10-raw_dataset.png)

---

# 🥉 BRONZE Layer

### Objective

Store source data in Delta format while preserving original records.

### Activities Performed

- Delta table creation
- Audit metadata generation
- Load timestamp capture
- Data lineage tracking

### Bronze Tables

```text
dim_users_bronze
dim_drivers_bronze
dim_locations_bronze
fact_rides_bronze
fact_payments_bronze
bronze_audit_log
```

### Screenshots

#### Bronze Catalog

![Bronze Catalog](screenshots/03_catalog_bronze.png)

#### Bronze Dataset

![Bronze Dataset](screenshots/11_bronze_dataset.png)

---

# 🥈 SILVER Layer

### Objective

Improve data quality and create analytics-ready datasets.

### Transformations Performed

#### Users

- Removed duplicate users
- Standardized city names
- Created signup year attributes

#### Drivers

- Removed duplicate drivers
- Validated ratings
- Created experience bands

#### Locations

- Standardized city and zone values

#### Rides

- Converted timestamps
- Created date dimensions
- Created completion and cancellation flags
- Added weekend indicators
- Added ride duration buckets

#### Payments

- Standardized payment data
- Created payment date attributes
- Validated payment amounts

### Silver Tables

```text
dim_users_silver
dim_drivers_silver
dim_locations_silver
fact_rides_silver
fact_payments_silver
silver_audit_log
```

### Screenshots

#### Silver Catalog

![Silver Catalog](screenshots/04_catalog_silver.png)

#### Silver Dataset

![Silver Dataset](screenshots/12_silver_datasets.png)

---

# 🥇 GOLD Layer

### Objective

Create business-facing analytical datasets for reporting and decision-making.

---

## Revenue Analytics

### gold_revenue_summary

Business KPIs:

- Total Revenue
- Total Rides
- Completion Rate
- Cancellation Rate
- Average Fare
- Average Distance

#### Screenshot

![Revenue Summary](screenshots/21_gold_revenue_summary.png)

---

## City Revenue Analysis

### gold_city_revenue

Business Questions:

- Which cities generate the most revenue?
- Which locations have the highest ride demand?

#### Screenshot

![City Revenue](screenshots/22_gold_top_cities_by_revenue.png)

---

## Driver Performance Analysis

### gold_driver_performance

Business Questions:

- Which drivers generate the most revenue?
- Which drivers have the highest completion rates?

#### Screenshot

![Driver Performance](screenshots/23_gold_top_drivers_by_revenue.png)

---

## Customer Analytics

### gold_customer_analytics

Business Questions:

- Who are the highest-value customers?
- Which customers spend the most?

#### Screenshot

![Customer Analytics](screenshots/24_gold_top_customers_by_spending.png)

---

## Demand Analytics

### gold_demand_by_hour

Business Questions:

- What are the peak ride hours?
- When does demand spike throughout the day?

#### Screenshot

![Demand Analytics](screenshots/25_gold_hourly_demand_pattern.png)

---

## Cancellation Analytics

### gold_cancellation_analysis

Business Questions:

- Why are rides cancelled?
- What are the most common cancellation reasons?

#### Screenshot

![Cancellation Analytics](screenshots/26_gold_cancellation_reasons.png)

---

## Vehicle Analytics

### gold_vehicle_performance

Business Questions:

- Which vehicle category performs best?
- Which vehicle type generates the highest revenue?

#### Screenshot

![Vehicle Analytics](screenshots/27_gold_vechile_performance.png)

---

## Executive Dashboard

### gold_executive_dashboard

Single-table KPI summary designed for business leadership.

KPIs Included:

- Total Users
- Total Drivers
- Total Revenue
- Total Rides
- Completion Rate
- Cancellation Rate
- Average Fare
- Average Distance

#### Screenshot

![Executive Dashboard](screenshots/28_gold_executive_dashboards.png)

---

# 📊 Business Value Delivered

The Gold Layer enables stakeholders to answer critical business questions:

### Revenue

- Revenue by city
- Revenue by vehicle type
- Revenue trends

### Operations

- Ride completion rates
- Cancellation trends
- Peak demand periods

### Drivers

- Driver productivity
- Revenue generation
- Service quality

### Customers

- Customer lifetime value
- Spending behavior
- Ride frequency

### Fleet

- Vehicle performance
- Fleet utilization

---

# 📚 Concepts Demonstrated

This project demonstrates practical implementation of:

- Medallion Architecture
- Delta Lake
- PySpark Transformations
- Data Quality Validation
- Audit Logging
- Data Lineage
- Fact & Dimension Modeling
- Business Aggregations
- Unity Catalog Governance
- ETL Pipeline Design
- Analytical Data Modeling

---

# 📁 Repository Structure

```text
ride_sharing_DE_analytics_lakehouse
│
├── docs
│   └── architecture.png
│
├── notebooks
│   ├── ride_raw.py
│   ├── ride_bronze.py
│   ├── ride_silver.py
│   └── ride_gold.py
│
├── screenshots
│
└── README.md
```

---

# 🚀 Key Takeaway

This project demonstrates how a modern Data Engineering platform can transform raw ride-sharing operational data into trusted business intelligence assets using Databricks, PySpark, Delta Lake, and Medallion Architecture principles.  

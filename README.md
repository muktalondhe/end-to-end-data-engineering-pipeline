# FMCG Data Engineering Lakehouse Project

An end-to-end Data Engineering project built using Databricks, Apache Spark, Python, SQL, and Amazon S3.  
This project simulates a real-world FMCG acquisition scenario where a large company acquires a startup and must consolidate messy, inconsistent datasets into a unified Lakehouse architecture.

---

# Project Overview

A company named **Atlon** acquires a startup called **Sports Bar**.  
Both organizations have different data standards, inconsistent schemas, duplicate records, and missing values.

The objective of this project is to:

- Build a scalable data pipeline
- Clean and transform raw business data
- Implement Medallion Architecture
- Perform historical backfills and incremental loading
- Create analytics-ready datasets
- Build BI dashboards for business insights

---

# Architecture

This project follows the **Medallion Architecture**:

## Bronze Layer
- Raw data ingestion
- Stores unprocessed source files
- Historical raw data preservation

## Silver Layer
- Data cleaning and transformation
- Schema standardization
- Deduplication
- Null handling
- Data quality checks

## Gold Layer
- Business-ready aggregated tables
- KPI calculations
- Dashboard-ready datasets

---

# Tech Stack

| Technology | Purpose |
|---|---|
| Python | ETL Development |
| SQL | Data Transformation |
| Apache Spark | Distributed Processing |
| Databricks Free Edition | Data Engineering Platform |
| Amazon S3 | Cloud Storage |
| Delta Lake | Lakehouse Storage Format |
| Power BI / Dashboard | Data Visualization |

---

# Project Workflow

## 1. Data Ingestion
- Load raw CSV files into Amazon S3
- Connect Databricks with S3
- Create Bronze tables

## 2. Data Cleaning
- Handle missing values
- Remove duplicates
- Standardize schemas
- Validate records

## 3. Dimension Table Processing
Processed:
- Customer Dimension
- Product Dimension
- Price Dimension

## 4. Fact Table Processing
- Historical backfill loading
- Incremental data loading
- Merge operations using Delta Lake

## 5. Gold Layer Creation
Created:
- Aggregated sales tables
- Business KPIs
- Denormalized reporting views

## 6. Dashboarding
- Interactive BI dashboard
- Cross-company sales analysis
- Revenue insights
- Product performance tracking

---

# Folder Structure

```bash
fmcg-data-engineering-lakehouse/
│
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── notebooks/
│   ├── bronze_layer/
│   ├── silver_layer/
│   ├── gold_layer/
│   └── dashboard_queries/
│
├── scripts/
│   ├── ingestion/
│   ├── transformations/
│   └── utilities/
│
├── dashboard/
│
├── images/
│
├── README.md
│
└── requirements.txt

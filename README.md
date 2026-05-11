# 🚲 Bike Data Lakehouse

> A production-style Data Lakehouse built from scratch using Databricks and the Medallion Architecture — covering raw ingestion, data quality, dimensional modelling, and automated pipelines.

---

## 📋 Project Overview

This project demonstrates a complete, real-world Data Lakehouse implementation using modern data engineering principles. Starting from raw CSV files, data is progressively transformed through **Bronze → Silver → Gold** layers, culminating in analytics-ready dimensional models and fully automated pipelines.

This is how modern data platforms are built in real companies.

---

## 🏗️ Architecture

```
Raw CSV Files (6 Source Files)
          │
          ▼
┌─────────────────────┐
│    BRONZE LAYER     │  Raw ingestion → Delta Tables (no transformations)
│  erp_* | crm_*     │  Source-prefixed table naming
└─────────────────────┘
          │
          ▼
┌─────────────────────┐
│    SILVER LAYER     │  Cleaned, validated, standardised, renamed
│  crm/* | erp/*     │  6 transformation notebooks
└─────────────────────┘
          │
          ▼
┌─────────────────────┐
│     GOLD LAYER      │  Star Schema — Fact & Dimension tables
│  fact_* | dim_*    │  Business-ready for analytics & BI
└─────────────────────┘
          │
          ▼
┌─────────────────────┐
│  DATABRICKS JOB     │  Automated end-to-end pipeline (scheduled daily)
│  Bronze→Silver→Gold │  Orchestration notebooks per layer
└─────────────────────┘
```

| Component | Tool |
|---|---|
| Platform | Databricks (Unity Catalog) |
| Processing Engine | Apache Spark (PySpark + Spark SQL) |
| Storage Format | Delta Lake |
| Orchestration | Databricks Jobs + Orchestration Notebooks |
| Version Control | Git / GitHub |
| Architecture | Medallion (Bronze / Silver / Gold) |

---

## 📦 Dataset

- **Domain:** Bike Sales Data
- **Source Files:** 6 CSV files (ERP and CRM source systems)
- **Volume Path:** `bronze.raw_sources`

---

## 🚀 Getting Started

### Prerequisites

- [ ] **Databricks Account** — [Sign Up for Free](https://www.databricks.com/try-databricks)
- [ ] **Git / GitHub Account** — [Create Account](https://github.com)
- [ ] New to Git? Start here:
  - [GitHub "Hello World" Guide](https://docs.github.com/en/get-started/quickstart/hello-world)
- [ ] **draw.io** (for architecture diagrams) — [Use Online](https://draw.io)

### Initial Setup

1. Clone this repository
2. In Databricks: **Workspace → Create → Git Folder** and connect via GitHub URL
3. Create the following schemas in Unity Catalog:
```sql
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
```
4. Create a volume inside the Bronze schema:
```sql
CREATE VOLUME bronze.raw_sources;
```
5. Upload all 6 CSV source files into the Bronze volume

---

## 📁 Repository Structure

```
bike-data-lakehouse/
│
├── bronze/
│   └── bronze_ingestion.ipynb          # Raw ingestion for all 6 sources
│
├── silver/
│   ├── crm/
│   │   ├── silver_crm_cust_info.ipynb
│   │   └── silver_crm_*.ipynb
│   ├── erp/
│   │   ├── silver_erp_*.ipynb
│   └── silver_orchestration.ipynb      # Triggers all Silver notebooks
│
├── gold/
│   ├── gold_dim_customers.ipynb
│   ├── gold_dim_products.ipynb
│   ├── gold_fact_sales.ipynb
│   └── gold_orchestration.ipynb        # Triggers all Gold notebooks
│
├── docs/
│   └── architecture_diagram.png
│
└── README.md
```

---

## 🛠️ Implementation Guide

### 🥉 Phase 1 — Project Initialisation

- Design and draw the Lakehouse architecture (draw.io)
- Create GitHub repository and connect to Databricks
- Create Unity Catalog schemas: `bronze`, `silver`, `gold`
- Create `raw_sources` volume inside the Bronze schema
- Upload all 6 CSV source files

**✅ Result:** Project is ready to start building layers.

---

### 🥉 Phase 2 — Bronze Layer (Raw Ingestion)

**Goal:** Ingest all raw CSV files into Delta tables with no transformations.

For each of the 6 CSV files:
- Read the CSV into a DataFrame
- Write to a Bronze Delta table using `overwrite` mode
- Use source-system prefix in table names (e.g. `erp_`, `crm_`)

```python
# Example pattern
df = spark.read.csv("/Volumes/bronze/raw_sources/filename.csv", header=True)
df.write.mode("overwrite").saveAsTable("bronze.erp_table_name")
```

> 💡 **Advanced Bonus:** Refactor with a dictionary of file paths and table names, then loop through to eliminate repeated code.

**✅ Result:** All 6 raw source files ingested into Bronze Delta tables.

---

### 🥈 Phase 3 — Silver Layer (Data Quality & Transformation)

**Goal:** Clean, validate, and standardise all Bronze data.

For each Bronze table, create a dedicated Silver notebook (`silver_<source>_<table_name>`):

**Data Quality Checks to perform:**
- 🔍 Find and remove duplicates
- 🔤 Fix extra whitespace and normalise abbreviations in string columns
- 📅 Validate and standardise date formats (String → Timestamp), handle nulls
- 🔢 Validate numeric values
- 🔑 Standardise business key IDs to ensure tables can be joined
- 📝 Rename all columns to clear, business-friendly names

**Notebook structure per Silver table:**
```
Section 1: Read from Bronze table into DataFrame
Section 2: Apply transformations (one at a time, display after each)
Section 3: Write to Silver table with friendly naming
Section 4: Sanity checks after write
```

> 💡 **Advanced Bonus:** Review all 6 notebooks and extract repeated logic into reusable Python functions or a shared config file.

**✅ Result:** All Bronze tables transformed into validated, analytics-ready Silver tables.

---

### 🥇 Phase 4 — Gold Layer (Dimensional Modelling)

**Goal:** Design and build a Star Schema for business intelligence.

**Data Model:**

```
         Dim_Customers
              │
Dim_Products──┼──Fact_Sales
              │
           Dim_Date
```

For each Gold table:
- Write a SQL query joining relevant Silver tables
- Validate for duplicates after joins
- Load into a DataFrame and write to a Gold Delta table
- Use `dim_` prefix for dimension tables and `fact_` for fact tables

> 💡 **Advanced Bonus — Data Product Ownership:**
> - Add meaningful descriptions to all Gold tables and columns in Unity Catalog
> - Define primary keys on dimension tables
> - Define foreign keys between fact and dimension tables

**✅ Result:** Business-ready Gold tables designed for analytics and reporting.

---

### ⚙️ Phase 5 — Pipeline Automation

**Goal:** Automate the full Bronze → Silver → Gold flow using Databricks Jobs.

**Step 1 — Create Orchestration Notebooks**

```python
# silver_orchestration.ipynb
dbutils.notebook.run("silver/crm/silver_crm_cust_info", timeout_seconds=300)
dbutils.notebook.run("silver/erp/silver_erp_table", timeout_seconds=300)
# ... repeat for all 6 Silver notebooks
```

```python
# gold_orchestration.ipynb
dbutils.notebook.run("gold/gold_dim_customers", timeout_seconds=300)
dbutils.notebook.run("gold/gold_dim_products", timeout_seconds=300)
dbutils.notebook.run("gold/gold_fact_sales", timeout_seconds=300)
```

**Step 2 — Create Databricks Job**

| Task | Notebook | Depends On |
|---|---|---|
| Task 1 | `bronze/bronze_ingestion` | — |
| Task 2 | `silver/silver_orchestration` | Task 1 |
| Task 3 | `gold/gold_orchestration` | Task 2 |

**Step 3 — Schedule & Monitor**
- Set a daily trigger on the job
- Monitor runs and check logs for the first few days
- Adjust or pause schedule as needed

**✅ Result:** Fully automated end-to-end Lakehouse pipeline running on a schedule.

---

## 🏆 Milestone Tracker

| Phase | Deliverable | Status |
|---|---|---|
| 01 | Project Initialisation & Architecture Design | ⬜ Not Started |
| 02 | Bronze Layer — Raw Ingestion | ⬜ Not Started |
| 03 | Silver Layer — Data Quality & Transformation | ⬜ Not Started |
| 04 | Gold Layer — Dimensional Modelling | ⬜ Not Started |
| 05 | Pipeline Automation & Scheduling | ⬜ Not Started |

---

## 🚀 Next Steps & Extensions

Once the core Lakehouse is complete, consider extending it with:

| Enhancement | Description |
|---|---|
| ✅ Data Quality Checks | Row counts, null checks, duplicate rules, business validation |
| ♻️ Reusable Code | Shared functions, config files, utility libraries |
| 🌐 New Data Sources | APIs, Kafka streams, operational databases |
| 🔄 CI/CD Pipelines | Automated testing, deployment, environment promotion |
| 🔒 Security & Governance | Access control, row-level security, data masking |
| 📊 Monitoring | Pipeline health dashboards, alerts, performance tracking |
| ⚡ Incremental Pipelines | CDC patterns, MERGE statements, real-time streaming |

---

## 🎓 Portfolio & Interview Tips

This project is a strong portfolio piece. When presenting it, be prepared to explain:

- **Why** you designed the Lakehouse with the Medallion Architecture
- **How** data flows from Bronze → Silver → Gold
- **How** you ensured data quality and scalability at each layer
- **How** you automated everything with orchestrated pipelines

Being able to walk through this confidently can be a strong differentiator in data engineering interviews.

---

## 📚 Resources

- [Medallion Architecture — Databricks Docs](https://www.databricks.com/glossary/medallion-architecture)
- [Data With Baraa — Databricks Live Bootcamp](https://www.youtube.com/@DataWithBaraa)
- [SQL Data Warehouse from Scratch — Full Hands-On Project](https://www.youtube.com/@DataWithBaraa)
- [GitHub Hello World Guide](https://docs.github.com/en/get-started/quickstart/hello-world)

---

## 🤝 Credits

Project concept and curriculum by **Data With Baraa**.  
Built independently as part of the Data With Baraa Lakehouse Bootcamp.

---

## 📄 License

This project is for educational and portfolio purposes.  
Please credit the original source if sharing publicly on GitHub or LinkedIn.

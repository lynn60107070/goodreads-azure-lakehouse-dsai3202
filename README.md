# Goodreads Azure Lakehouse — DSAI 3202 Lab 3

## Overview
End-to-end data engineering project implementing a **medallion architecture (Bronze → Silver → Gold)** for Goodreads reviews.  
Developed as part of **Cloud Computing Lab 3** (University of Doha for Science and Technology, DSAI 3202).

The repository demonstrates:
- Ingestion and cleaning of Goodreads review data using PySpark
- Integration with **Azure Data Lake Storage Gen2**
- Preprocessing and feature enrichment in **Microsoft Fabric** and **Databricks**
- Reproducible, Pythonic folder structure aligned with academic and industry standards

---

## Repository Structure
goodreads-azure-lakehouse-dsai3202/
├─ data/ # Layered storage following Medallion architecture
│ ├─ bronze/ # Raw data as ingested from ADLS (unprocessed)
│ │ ├─ .gitkeep # Placeholder to keep folder tracked in Git
│ │ └─ README.md # Explains purpose and schema of the Bronze layer
│ ├─ silver/ # Cleaned and validated Parquet data (post-processing)
│ │ ├─ .gitkeep
│ │ └─ README.md # Documents cleaning and validation operations
│ └─ gold/ # Curated + feature-enriched Delta datasets
│ ├─ .gitkeep
│ └─ README.md # Describes curated/feature tables and schema
│
├─ doc/ # Formal documentation and audit evidence
│ ├─ fabric_steps.md # Full record of Microsoft Fabric preprocessing steps
│ ├─ lab_report.md # Official Lab 3 summary written for grading submission
│ └─ sql_and_m_snippets.md # Contains SQL and Power Query M transformation code
│
├─ notebooks/ # Evidence notebooks for both homeworks
│ ├─ homework1/ # Homework 1: Databricks cleaning & curation
│ │ └─ lab3_databricks_hw1.ipynb # Notebook implementing Spark cleaning logic
│ └─ homework2/ # Homework 2: Fabric aggregation & enrichment
│ ├─ fabric_dataflow_gen2.json # Export of published Microsoft Fabric Dataflow
│ └─ lab3_fabric_hw2.ipynb # Databricks verification of Fabric results
│
├─ src/ # Core modular Python source code
│ ├─ __init__.py # Marks directory as a package
│ ├─ clean_reviews.py # Functions for cleaning, trimming, deduplication
│ ├─ config.py # Central configuration (paths, storage URIs)
│ ├─ curate_gold.py # Joins datasets into curated Gold Delta tables
│ ├─ features.py # Builds aggregate and enrichment features
│ ├─ io.py # Reusable read/write helpers for Parquet and Delta
│ └─ spark_session.py # Initializes SparkSession with Delta + Azure configs
│
├─ .gitattributes # Defines Git file-handling and line-ending normalization
├─ .gitignore # Excludes temporary, log, and environment files from Git
├─ LICENSE # MIT license granting open-source usage rights
├─ main.py # Main entry point orchestrating the entire pipeline
├─ README.md # Project documentation (this file)
└─ requirements.txt # Lists all Python dependencies with pinned versions

---

## Data Flow Summary
| Layer | Description | Tools Used |
|-------|--------------|-------------|
| **Bronze** | Raw data ingested from ADLS (unprocessed). | Azure Data Lake |
| **Silver** | Cleaned and validated Parquet data. | Databricks (PySpark) |
| **Gold** | Curated & feature-enriched dataset. | Fabric + Databricks |

---

## Homework Integration
- **Homework 1 (Databricks)** → Cleaning and curation steps → stored under `notebook/homework1/`
- **Homework 2 (Fabric)** → Aggregations and enrichment → stored under `notebook/homework2/`

---

## Execution (CMD or Databricks)
```bash
# Clone repository
git clone https://github.com/<your-username>/goodreads-azure-lakehouse-dsai3202.git
cd goodreads-azure-lakehouse-dsai3202

# Run full pipeline
python main.py

## Requirements
- **Python:** ≥ 3.9  
- **PySpark:** ≥ 3.4  
- **Delta Lake:** ≥ 2.4  
- **Access:** Azure Data Lake Storage Gen2  

---

## Documentation Access
- Detailed Fabric workflow → [doc/fabric_steps.md](doc/fabric_steps.md)
- Formal lab report → [doc/lab_report.md](doc/lab_report.md)
- SQL & M code snippets → [doc/sql_and_m_snippets.md](doc/sql_and_m_snippets.md)

---

## Academic Compliance
This repository fulfills all deliverables required for  
**Lab 3 — Data Preprocessing on Azure**, demonstrating:

- Bronze, Silver, and Gold layer design  
- Data cleaning, curation, and enrichment workflows  
- Documentation and reproducibility standards  
- Integration of Microsoft Fabric and Databricks processes  

---

**Prepared by:** Lynn Younes 60107070
**Course:** DSAI 3202 — Cloud Computing  
**Institution:** University of Doha for Science and Technology

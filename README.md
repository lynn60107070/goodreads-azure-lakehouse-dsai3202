# Goodreads Azure Lakehouse — DSAI 3202 Lab 3

## Overview
End-to-end data engineering project implementing a **Medallion architecture (Bronze → Silver → Gold)** for Goodreads reviews.  
Developed for **Cloud Computing Lab 3** (UDST, DSAI 3202).

The repository demonstrates:
- Ingestion & cleaning of Goodreads review data using PySpark
- Integration with **Azure Data Lake Storage Gen2**
- Preprocessing & feature enrichment in **Microsoft Fabric** and **Databricks**
- Reproducible, Pythonic folder structure aligned with academic + industry standards

---

## Repository Structure
```text
goodreads-azure-lakehouse-dsai3202/
├─ data/                         # Layered storage (Medallion)
│  ├─ bronze/                    # Raw data from ADLS (unprocessed)
│  │  ├─ .gitkeep
│  │  └─ README.md               # Bronze purpose & schema
│  ├─ silver/                    # Cleaned & validated Parquet
│  │  ├─ .gitkeep
│  │  └─ README.md               # Cleaning & validation notes
│  └─ gold/                      # Curated + feature-enriched Delta
│     ├─ .gitkeep
│     └─ README.md               # Curated tables & schema
│
├─ doc/                          # Formal documentation & audit evidence
│  ├─ fabric_steps.md            # Microsoft Fabric preprocessing steps
│  ├─ lab_report.md              # Official Lab 3 summary
│  └─ sql_and_m_snippets.md      # SQL and Power Query M code
│
├─ notebooks/                    # Evidence notebooks
│  ├─ homework1/                 # HW1: Databricks cleaning & curation
│  │  └─ lab3_databricks_hw1.ipynb
│  └─ homework2/                 # HW2: Fabric aggregation & enrichment
│     ├─ fabric_dataflow_gen2.json
│     └─ lab3_fabric_hw2.ipynb   # Databricks verification of Fabric results
│
├─ src/                          # Core modular Python code
│  ├─ __init__.py
│  ├─ clean_reviews.py           # Cleaning, trimming, deduplication
│  ├─ config.py                  # Paths, storage URIs
│  ├─ curate_gold.py             # Joins → curated Gold Delta tables
│  ├─ features.py                # Aggregates & enrichment features
│  ├─ io.py                      # Reusable read/write helpers
│  └─ spark_session.py           # SparkSession (Delta + Azure configs)
│
├─ .gitattributes                # Line-endings & file normalization
├─ .gitignore                    # Ignore temp/log/env files
├─ LICENSE                       # MIT license
├─ main.py                       # Orchestrates the pipeline
├─ README.md                     # This file
└─ requirements.txt              # Pinned Python dependencies
```
---

## Data Flow Summary
| Layer | Description | Tools Used |
|-------|--------------|-------------|
| **Bronze** | Raw data ingested from ADLS (unprocessed) | Azure Data Lake |
| **Silver** | Cleaned and validated Parquet data | Databricks (PySpark) |
| **Gold** | Curated & feature-enriched Delta datasets | Fabric + Databricks |

---

## Homework Integration
- **Homework 1 (Databricks)** → Cleaning & curation → `notebooks/homework1/`  
- **Homework 2 (Fabric)** → Aggregations & enrichment → `notebooks/homework2/`

---

## Execution (CMD or Databricks)
```bash
# Clone repository
git clone https://github.com/<your-username>/goodreads-azure-lakehouse-dsai3202.git
cd goodreads-azure-lakehouse-dsai3202

# Run full pipeline
python main.py
---

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
This repository fulfills all deliverables required for **Lab 3 — Data Preprocessing on Azure**, demonstrating:

- Bronze, Silver, and Gold layer design  
- Data cleaning, curation, and enrichment workflows  
- Documentation and reproducibility standards  
- Integration of Microsoft Fabric and Databricks processes  

---

**Prepared by:** Lynn Younes 60107070  
**Course:** DSAI 3202 — Cloud Computing  
**Institution:** University of Doha for Science and Technology

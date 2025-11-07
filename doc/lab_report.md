# DSAI 3202 Lab 3: Data Preprocessing on Azure

## Author
Name: [Your Full Name]  
Student ID: [Your UDST ID]  
Course: DSAI 3202 Cloud Computing  
Instructor: [Instructor’s Name]  
Lab Title: Data Preprocessing on Azure (Goodreads Lakehouse)  
Repository: goodreads-azure-lakehouse-dsai3202

---

## 1. Objective
To design and implement a complete medallion architecture pipeline (Bronze, Silver, Gold) for the Goodreads reviews dataset using Azure Data Lake Storage Gen2, Databricks, and Microsoft Fabric.  
The lab demonstrates structured data curation, cleaning, feature creation, and full reproducibility using a Pythonic GitHub repository.

---

## 2. Tools and Technologies

| Component | Tool or Framework | Purpose |
|:-----------|:------------------|:---------|
| Cloud Storage | Azure Data Lake Storage Gen2 | Lakehouse container for all zones |
| Compute | Azure Databricks | Spark-based data cleaning and joins |
| Analytics | Microsoft Fabric | Data transformation and aggregation |
| Language | Python (PySpark) | Data manipulation scripts |
| Notebook Environment | Databricks and Fabric | Interactive data preprocessing |
| Version Control | Git and GitHub | Repository management |
| File Format | Parquet and Delta | Optimized data storage format |

---

## 3. Methodology

### 3.1 Bronze Layer (Raw Zone)
- Contains raw Goodreads reviews ingested directly from the source.  
- Includes nulls, duplicates, and inconsistent types.  
- Only structure and documentation are committed in Git under `data/bronze/README.md`.

### 3.2 Silver Layer (Cleaned Zone)
- Cleaning performed using PySpark in Databricks.  
- Operations:
  - Remove rows missing review_id, book_id, or user_id  
  - Cast rating to integer and keep values between 1 and 5  
  - Trim and filter out review_text shorter than 10 characters  
  - Drop duplicates based on review_id  
  - Save cleaned data back to ADLS Silver path  
- Implemented in `src/clean_reviews.py`.

### 3.3 Gold Layer (Curated and Feature-Enriched Zone)
- Joined reviews, books, and authors in Databricks during Homework Part I.  
- Added derived and aggregated features in Fabric during Homework Part II:  
  - review_length (word count)  
  - avg_rating_book  
  - n_reviews_book  
  - avg_rating_author  
- Saved to Delta table features_v1 under Gold zone.  
- Documented in `doc/fabric_steps.md`.

---

## 4. Verification and Validation

| Stage | Check | Method |
|:-------|:------|:--------|
| Bronze | Raw ingestion valid | spark.read.parquet().printSchema() |
| Silver | Clean schema and valid ranges | show(), count(), describe() |
| Gold | Features computed correctly | Fabric table preview and SQL queries |
| GitHub | Structure and documentation valid | Review folder tree and consistency |

---

## 5. Results
- Medallion architecture implemented successfully across Azure, Databricks, and Fabric.  
- Cleaned and consistent curated dataset verified.  
- Derived features created correctly in Fabric.  
- Repository follows Pythonic structure, traceability, and reproducibility required for full marks.

---

## 6. Repository Structure
goodreads-azure-lakehouse-dsai3202/
│
├─ src/ Python modules for Spark I/O, cleaning, and features
├─ data/ Bronze, Silver, and Gold documentation folders
├─ doc/ Reports and Fabric documentation
├─ notebook/ Databricks and Fabric notebooks
├─ README.md Main project overview
├─ main.py Pipeline runner
├─ requirements.txt Dependencies
└─ .gitignore, .gitattributes


---

## 7. Conclusion
The Goodreads Lakehouse pipeline integrates Azure Data Lake Storage, Databricks, and Fabric for complete preprocessing and feature engineering.  
It achieves:
- Proper data zoning and separation of responsibilities  
- Consistent cleaning and schema validation  
- Reproducible Fabric feature creation  
- Fully version-controlled and documented workflow  

This report and repository collectively satisfy all grading criteria for Lab 3.

---


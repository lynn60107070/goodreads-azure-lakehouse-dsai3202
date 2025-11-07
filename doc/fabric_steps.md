# Microsoft Fabric Data Preprocessing — Steps Summary

## Purpose
Describes all steps executed in **Microsoft Fabric** for **Homework Part II (Data Preprocessing and Feature Enrichment)**.  
Provides an auditable record of Power Query M transformations, aggregations, and publication workflow applied to the curated dataset in Azure Data Lake Storage Gen2.

---

## 1. Workspace Setup
- Created workspace `goodreads-ws-60XXXXXX`  
- Activated Fabric Trial Capacity  
- Verified link to ADLS Gen2 container  

**Connection Path:**  
`https://goodreadsreviews60XXXXXX.dfs.core.windows.net/lakehouse/gold/curated_reviews/`

---

## 2. Dataflow Gen2 Creation
- Opened **New → Dataflow Gen2** in Fabric workspace  
- Added Blank Query → connected to ADLS Gen2 via Access Key  
- Used the following M code to load the Delta table:

```m
let
    Source = AzureStorage.DataLake(
        "https://goodreadsreviews60XXXXXX.dfs.core.windows.net/lakehouse/gold/curated_reviews/",
        [HierarchicalNavigation = true]
    ),
    DeltaTable = DeltaLake.Table(Source)
in
    DeltaTable
```

Verified Columns:
`review_id`, `book_id`, `title`, `author_id`, `name`, `user_id`, `rating`, `review_text`


## 3. Cleaning Steps in Power Query
- Adjusted column data types (text, numeric, date)  
- Removed rows with null or invalid values for: `rating`, `book_id`, `review_text`  
- Trimmed text columns (`title`, `name`, `review_text`)  
- Standardized casing → *Capitalize Each Word* on `title`, `name`  
- Dropped reviews with `review_length < 10` characters  
- Replaced missing values:  
  - `n_votes = 0`  
  - `language = "Unknown"`  
- Validated `date_added`; removed future dates  

---

## 4. Feature Aggregation and Enrichment
Used **Group By** to compute:
- Average rating per `BookID`  
- Number of reviews per `BookID`  
- Average rating per `AuthorName`  
- Word count statistics on `review_text`

**Derived Columns:**
- `review_length` = word count  
- `avg_rating_book`, `n_reviews_book` (aggregates)

---

## 5. Publication
- Selected **Next → Publish**  
- Destination → *Warehouse / Datamart (Preview)*  
- Published table name: `curated_reviews`  
- Verified refresh and previewed final table  

---

## 6. Validation
- Confirmed schema alignment with Databricks curated reviews  
- Verified data types, null handling, and numeric ranges  
- Ensured derived features persisted correctly in Warehouse  

---

## 7. Repository Artifacts

| Stage | File / Notebook | Description |
|:------|:----------------|:-------------|
| Fabric Dataflow | `notebook/homework2_fabric/dataflow_gen2_export.json` | JSON export of Dataflow Gen2 |
| Power Query M | `notebook/homework2_fabric/power_query_M.m` | Transformation script |
| Documentation | `doc/fabric_steps.md` | Narrative summary (this file) |
| Evidence | `notebook/homework2_fabric/evidence/` | Screenshots of Fabric UI and published table |

---

## Notes
- Fabric preprocessing mirrors Databricks cleaning logic to maintain schema consistency.  
- This documentation ensures full traceability and meets all requirements for Homework Part II grading and audit.

\# Databricks Metadata-Driven Pipeline



A production-ready metadata-driven ETL pipeline for Databricks.



\## Architecture



\- \*\*Metadata\*\*: YAML config (can be migrated to Delta table)

\- \*\*State Management\*\*: Delta table for watermarks

\- \*\*Execution\*\*: Databricks notebooks/jobs

\- \*\*Write Modes\*\*: Append, Overwrite, Merge (Upsert)



\## Setup



1\. Clone this repo to Databricks Repos

2\. Upload `data/sales\_orders.csv` to DBFS `/FileStore/`

3\. Run `notebooks/run\_pipeline.py`



\## Usage



```python

\# Run all datasets

pipeline = MetadataPipeline(spark)

pipeline.run\_all()



\# Run specific dataset

pipeline.run\_dataset("sales\_orders")


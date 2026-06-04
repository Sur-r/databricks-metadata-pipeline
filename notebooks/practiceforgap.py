# Databricks notebook - run_pipeline
# This is the entry point for Databricks execution

# MAGIC %md
# # Metadata-Driven Pipeline Runner
# ## Execute this notebook to run your pipeline

# MAGIC %md
# ### 1. Setup and Imports

import sys
import os

# Add project root to path (so we can import src modules)
project_root = "/Workspace/Repos/YOUR_USERNAME/databricks-metadata-pipeline"
sys.path.insert(0, project_root)

from src.orchestrator import MetadataPipeline

# MAGIC %md
# ### 2. Configure Parameters

# Create widgets for parameter passing
try:
    dbutils.widgets.text("dataset_name", "", "Dataset Name (leave empty for all)")
    dbutils.widgets.text("catalog", "default", "Catalog Name")
    dbutils.widgets.text("schema", "control", "Control Schema Name")
except:
    pass  # Running as job

# Get parameters
dataset_name = dbutils.widgets.get("dataset_name") if "dbutils" in dir() else ""
catalog = dbutils.widgets.get("catalog") if "dbutils" in dir() else "default"
schema = dbutils.widgets.get("schema") if "dbutils" in dir() else "control"

print(f"📋 Configuration:")
print(f"   Dataset filter: {dataset_name or 'ALL'}")
print(f"   Catalog: {catalog}")
print(f"   Schema: {schema}")

# MAGIC %md
# ### 3. Initialize and Run Pipeline

# Create pipeline instance
pipeline = MetadataPipeline(spark, catalog, schema)

# Run pipeline
if dataset_name and dataset_name.strip():
    # Run single dataset
    result = pipeline.run_dataset(dataset_name.strip())
    print(f"\n📊 Final result: {result}")
else:
    # Run all enabled datasets
    results = pipeline.run_all()
    print(f"\n📊 Final results:")
    for r in results:
        print(f"   {r}")

# MAGIC %md
# ### 4. Verify Results

# Check watermark table
print("\n📊 Watermark State:")
display(spark.sql(f"SELECT * FROM {catalog}.{schema}.watermark_state"))
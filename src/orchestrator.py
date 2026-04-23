"""
Main orchestrator - ties everything together
"""
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession

# Import our modules
from src.config_loader import load_config, get_enabled_datasets
from src.sources import read_source
from src.targets import write_target, compute_max_watermark

class WatermarkManager:
    """
    Manages watermark state using Delta tables
    In production, this would read/write to a control table
    """
    
    def __init__(self, spark: SparkSession, catalog: str = "default", schema: str = "control"):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self._ensure_watermark_table()
    
    def _ensure_watermark_table(self):
        """Create watermark table if it doesn't exist"""
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.watermark_state (
                dataset_name STRING,
                last_watermark_value STRING,
                last_run_timestamp TIMESTAMP,
                records_processed BIGINT,
                status STRING
            ) USING DELTA
        """
        self.spark.sql(create_table_sql)
        print(f"✅ Watermark table ready: {self.catalog}.{self.schema}.watermark_state")
    
    def get_watermark(self, dataset_name: str) -> Optional[str]:
        """Get last watermark value for a dataset"""
        result = self.spark.sql(f"""
            SELECT last_watermark_value 
            FROM {self.catalog}.{self.schema}.watermark_state 
            WHERE dataset_name = '{dataset_name}'
            ORDER BY last_run_timestamp DESC
            LIMIT 1
        """)
        
        if result.count() == 0:
            return None
        return result.collect()[0][0]
    
    def update_watermark(self, dataset_name: str, watermark_value: str, records: int, status: str = "success"):
        """Update watermark after successful run"""
        self.spark.sql(f"""
            INSERT INTO {self.catalog}.{self.schema}.watermark_state
            VALUES (
                '{dataset_name}',
                '{watermark_value}',
                CURRENT_TIMESTAMP(),
                {records},
                '{status}'
            )
        """)
        print(f"  💧 Updated watermark: {watermark_value}")

class MetadataPipeline:
    """Main pipeline orchestrator"""
    
    def __init__(self, spark: SparkSession, catalog: str = "default", schema: str = "control"):
        self.spark = spark
        self.watermark_manager = WatermarkManager(spark, catalog, schema)
    
    def run_dataset(self, dataset_name: str) -> Dict[str, Any]:
        """
        Run pipeline for a single dataset
        
        Returns:
            Dictionary with run statistics
        """
        print(f"\n{'='*60}")
        print(f"🚀 Starting pipeline for: {dataset_name}")
        print(f"{'='*60}")
        
        # 1. Load configuration
        config = load_config(dataset_name)
        
        # 2. Check if enabled
        if not config.get('enabled', True):
            print(f"⏭️  Dataset '{dataset_name}' is disabled. Skipping.")
            return {"dataset": dataset_name, "status": "skipped"}
        
        # 3. Get last watermark (for incremental loads)
        last_watermark = None
        if config.get('load_type') == 'incremental':
            last_watermark = self.watermark_manager.get_watermark(dataset_name)
            print(f"📊 Last watermark: {last_watermark}")
        
        # 4. Read source data
        df = read_source(self.spark, config, last_watermark)
        record_count = df.count()
        
        # 5. If no new data, exit early
        if record_count == 0:
            print(f"✅ No new records to process. Exiting.")
            return {"dataset": dataset_name, "status": "no_new_data", "records": 0}
        
        print(f"📊 Processing {record_count} new records")
        
        # 6. Write to target
        write_target(self.spark, df, config)
        
        # 7. Update watermark
        if config.get('load_type') == 'incremental':
            new_watermark = compute_max_watermark(df, config)
            if new_watermark:
                self.watermark_manager.update_watermark(
                    dataset_name, 
                    new_watermark, 
                    record_count,
                    "success"
                )
        
        print(f"{'='*60}")
        print(f"✅ Successfully completed: {dataset_name}")
        print(f"{'='*60}\n")
        
        return {
            "dataset": dataset_name,
            "status": "success",
            "records": record_count
        }
    
    def run_all(self) -> list:
        """Run all enabled datasets"""
        datasets = get_enabled_datasets()
        print(f"\n📋 Found {len(datasets)} enabled datasets: {datasets}")
        
        results = []
        for dataset_name in datasets:
            try:
                result = self.run_dataset(dataset_name)
                results.append(result)
            except Exception as e:
                print(f"❌ ERROR processing {dataset_name}: {str(e)}")
                results.append({
                    "dataset": dataset_name,
                    "status": "failed",
                    "error": str(e)
                })
                # Update watermark with failure
                self.watermark_manager.update_watermark(
                    dataset_name, 
                    None, 
                    0, 
                    f"failed: {str(e)}"
                )
        
        return results
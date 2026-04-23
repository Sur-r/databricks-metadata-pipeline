"""
Dynamic target writer - writes to Delta tables with different modes
"""
from typing import Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import max as spark_max
from delta.tables import DeltaTable

def write_target(
    spark: SparkSession,
    df: DataFrame,
    config: Dict[str, Any]
) -> None:
    """
    Write DataFrame to target based on configuration
    
    Args:
        spark: SparkSession
        df: DataFrame to write
        config: Dataset configuration from metadata
    """
    target_path = config['target_path']
    write_mode = config.get('write_mode', 'append')
    target_type = config.get('target_type', 'delta')
    
    print(f"  💾 Writing to: {target_path}")
    print(f"  📝 Write mode: {write_mode}")
    
    if target_type != 'delta':
        raise ValueError(f"Only 'delta' target type supported. Got: {target_type}")
    
    # Handle different write modes
    if write_mode == 'overwrite':
        # Replace entire table
        df.write.format("delta").mode("overwrite").save(target_path)
        print(f"  ✅ Overwrote {df.count()} records")
    
    elif write_mode == 'append':
        # Add new records
        df.write.format("delta").mode("append").save(target_path)
        print(f"  ✅ Appended {df.count()} records")
    
    elif write_mode == 'merge':
        # Upsert based on primary keys
        primary_keys = config.get('primary_keys')
        
        if not primary_keys:
            raise ValueError("primary_keys required for merge mode")
        
        print(f"  🔑 Merge keys: {primary_keys}")
        
        # Check if target table exists
        if not DeltaTable.isDeltaTable(spark, target_path):
            # First run: just create the table
            print(f"  🆕 Creating new Delta table")
            df.write.format("delta").save(target_path)
        else:
            # Perform merge (upsert)
            delta_table = DeltaTable.forPath(spark, target_path)
            
            # Build merge condition
            merge_condition = " AND ".join([
                f"target.{key} = source.{key}" 
                for key in primary_keys
            ])
            
            # Execute merge
            (delta_table.alias("target")
             .merge(df.alias("source"), merge_condition)
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute())
            
            print(f"  ✅ Merged {df.count()} records")
    
    else:
        raise ValueError(f"Unknown write_mode: {write_mode}")

def compute_max_watermark(df: DataFrame, config: Dict[str, Any]) -> Optional[str]:
    """
    Compute maximum watermark value from DataFrame
    
    Args:
        df: DataFrame with watermark column
        config: Dataset configuration
    
    Returns:
        Maximum watermark value as string, or None if no rows
    """
    if config.get('load_type') != 'incremental':
        return None
    
    watermark_col = config.get('watermark_column')
    if not watermark_col or df.count() == 0:
        return None
    
    max_value = df.agg(spark_max(watermark_col)).collect()[0][0]
    return str(max_value) if max_value else None
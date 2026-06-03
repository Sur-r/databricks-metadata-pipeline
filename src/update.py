"""
Dynamic source reader - reads different file formats based on metadata
"""
from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

def read_source(
    spark: SparkSession,
    config: Dict[str, Any],
    last_watermark: Optional[str] = None
) -> DataFrame:
    """
    Read source data based on configuration
    
    Args:
        spark: SparkSession
        config: Dataset configuration from metadata
        last_watermark: Previous watermark value (for incremental loads)
    
    Returns:
        DataFrame with source data (filtered if incremental)
    """
    source_type = config['source_type']
    source_path = config['source_path']
    
    print(f"  📖 Reading from: {source_path}")
    print(f"  📄 Source type: {source_type}")
    
    # Read based on source type
    if source_type == 'csv':
        df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(source_path))
    
    elif source_type == 'parquet':
        df = spark.read.parquet(source_path)
    
    elif source_type == 'delta':
        df = spark.read.format("delta").load(source_path)
    
    elif source_type == 'json':
        df = spark.read.option("inferSchema", "true").json(source_path)
    
    else:
        raise ValueError(f"Unsupported source type: {source_type}")
    
    # Apply incremental filter if needed
    if (config.get('load_type') == 'incremental' and 
        last_watermark is not None and 
        config.get('watermark_column')):
        
        watermark_col = config['watermark_column']
        print(f"  🔍 Applying incremental filter: {watermark_col} > {last_watermark}")
        
        # Ensure watermark column exists
        if watermark_col not in df.columns:
            raise ValueError(f"Watermark column '{watermark_col}' not found in source")
        
        # Filter to only new records
        df = df.filter(col(watermark_col) > last_watermark)
    
    print(f"  ✅ Loaded {df.count()} records")
    return df
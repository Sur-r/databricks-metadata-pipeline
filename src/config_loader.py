"""
Metadata config loader - reads from YAML file
In production, this would read from a Delta table
"""
import os
import yaml
from typing import Dict, Any, Optional

# Get the project root directory
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(PROJECT_ROOT, "metadata", "pipeline_config.yaml")

def load_all_configs() -> Dict[str, Any]:
    """
    Load all dataset configurations from YAML file
    Returns: Dictionary with dataset_name as key
    """
    with open(CONFIG_PATH, 'r') as f:
        config = yaml.safe_load(f)
    
    datasets = config.get('datasets', [])
    # Convert list to dict keyed by dataset_name for easy lookup
    return {d['dataset_name']: d for d in datasets}

def load_config(dataset_name: str) -> Dict[str, Any]:
    """Load configuration for a specific dataset"""
    all_configs = load_all_configs()
    
    if dataset_name not in all_configs:
        raise ValueError(f"Dataset '{dataset_name}' not found in metadata config")
    
    return all_configs[dataset_name]

def get_enabled_datasets() -> list:
    """Get list of all enabled dataset names"""
    all_configs = load_all_configs()
    return [name for name, cfg in all_configs.items() if cfg.get('enabled', True)]
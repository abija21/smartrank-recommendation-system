from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class PipelineConfig:
    """Pipeline configuration settings"""
    
    # Data paths
    raw_data_path: str = "./data"
    processed_data_path: str = "./processed_data"
    
    # Spark settings
    spark_app_name: str = "SmartRank-DataPipeline"
    spark_executor_memory: str = "2g"
    spark_driver_memory: str = "1g"
    
    # Feature engineering
    tfidf_num_features: int = 1000
    sample_fraction: float = 0.1
    sample_size: int = 1000
    
    # Data validation
    min_rating: float = 1.0
    max_rating: float = 5.0
    max_price: float = 10000.0
    
    def to_spark_config(self) -> Dict[str, Any]:
        """Convert to Spark configuration dictionary"""
        return {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.executor.memory": self.spark_executor_memory,
            "spark.driver.memory": self.spark_driver_memory,
        }
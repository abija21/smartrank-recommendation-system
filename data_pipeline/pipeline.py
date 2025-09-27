import logging
import os
from pyspark.sql import SparkSession

from .config import PipelineConfig
from .data_loader import DataLoader
from .data_processor import DataProcessor
from .feature_engineer import FeatureEngineer

logger = logging.getLogger(__name__)

class SmartRankPipeline:
    """Main data pipeline orchestrator"""
    
    def __init__(self, config: PipelineConfig = None):
        self.config = config or PipelineConfig()
        self.spark = self._create_spark_session()
        
        # Initialize components
        self.loader = DataLoader(self.spark, self.config)
        self.processor = DataProcessor(self.config)
        self.feature_engineer = FeatureEngineer(self.config)
    
    def _create_spark_session(self) -> SparkSession:
        """Create optimized Spark session"""
        builder = SparkSession.builder.appName(self.config.spark_app_name)
        
        for key, value in self.config.to_spark_config().items():
            builder = builder.config(key, value)
            
        return builder.getOrCreate()
    
    def run(self, reviews_file: str = "All_Beauty.jsonl", metadata_file: str = "meta_All_Beauty.jsonl") -> bool:
        """Execute the complete pipeline"""
        try:
            # Construct file paths
            reviews_path = os.path.join(self.config.raw_data_path, reviews_file)
            metadata_path = os.path.join(self.config.raw_data_path, metadata_file)
            
            # Validate input files
            self._validate_input_files(reviews_path, metadata_path)
            
            # Load raw data
            logger.info("Starting data pipeline...")
            reviews_df = self.loader.load_reviews(reviews_path)
            metadata_df = self.loader.load_metadata(metadata_path)
            
            # Clean data
            reviews_df = self.processor.clean_reviews(reviews_df)
            metadata_df = self.processor.clean_metadata(metadata_df)
            
            # Engineer features
            features_df, user_stats_df = self.feature_engineer.create_product_features(reviews_df, metadata_df)
            
            # Save processed data
            self._save_processed_data(reviews_df, metadata_df, features_df, user_stats_df)
            
            # Generate summary
            self._print_summary(reviews_df, metadata_df)
            
            logger.info("Pipeline completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            return False
        finally:
            self.spark.stop()
    
    def _validate_input_files(self, reviews_path: str, metadata_path: str):
        """Validate input files exist"""
        if not os.path.exists(reviews_path):
            raise FileNotFoundError(f"Reviews file not found: {reviews_path}")
        if not os.path.exists(metadata_path):
            raise FileNotFoundError(f"Metadata file not found: {metadata_path}")
    
    def _save_processed_data(self, reviews_df, metadata_df, features_df, user_stats_df):
        """Save all processed datasets"""
        os.makedirs(self.config.processed_data_path, exist_ok=True)
        
        output_path = self.config.processed_data_path
        
        # Save main datasets
        reviews_df.coalesce(4).write.mode("overwrite").parquet(f"{output_path}/reviews")
        metadata_df.coalesce(2).write.mode("overwrite").parquet(f"{output_path}/metadata")
        features_df.coalesce(2).write.mode("overwrite").parquet(f"{output_path}/product_features")
        user_stats_df.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/user_stats")
        
        # Save sample for testing
        sample_reviews = reviews_df.sample(self.config.sample_fraction, seed=42).limit(self.config.sample_size)
        sample_reviews.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/sample_reviews")
        
        logger.info(f"Processed data saved to {output_path}")
    
    def _print_summary(self, reviews_df, metadata_df):
        """Print concise pipeline summary"""
        print("\n" + "="*50)
        print("SMARTRANK PIPELINE SUMMARY")
        print("="*50)
        print(f"Reviews processed: {reviews_df.count():,}")
        print(f"Products processed: {metadata_df.count():,}")
        print(f"Data saved to: {self.config.processed_data_path}")
        print("="*50)
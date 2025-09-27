import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, from_unixtime, lit

from .config import PipelineConfig

logger = logging.getLogger(__name__)

class DataProcessor:
    """Handles data cleaning and validation"""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
    
    def clean_reviews(self, reviews_df: DataFrame) -> DataFrame:
        """Clean and validate reviews data"""
        initial_count = reviews_df.count()
        
        # Filter out invalid records
        cleaned_df = reviews_df.filter(
            col("user_id").isNotNull() &
            col("parent_asin").isNotNull() & 
            col("rating").isNotNull() &
            (col("rating") >= self.config.min_rating) &
            (col("rating") <= self.config.max_rating)
        )
        
        # Convert timestamp to readable date
        cleaned_df = cleaned_df.withColumn(
            "review_date", 
            when(col("timestamp") > 0, from_unixtime(col("timestamp") / 1000).cast("date"))
            .otherwise(lit("1970-01-01").cast("date"))
        )
        
        final_count = cleaned_df.count()
        logger.info(f"Reviews cleaned: {initial_count} -> {final_count} (removed {initial_count - final_count})")
        
        return cleaned_df
    
    def clean_metadata(self, metadata_df: DataFrame) -> DataFrame:
        """Clean and validate metadata"""
        initial_count = metadata_df.count()
        
        # Filter records with missing parent_asin
        cleaned_df = metadata_df.filter(col("parent_asin").isNotNull())
        
        # Clean price field
        cleaned_df = cleaned_df.withColumn(
            "price_clean",
            when((col("price") > 0) & (col("price") < self.config.max_price), col("price"))
            .otherwise(0.0)
        )
        
        final_count = cleaned_df.count()
        logger.info(f"Metadata cleaned: {initial_count} -> {final_count} (removed {initial_count - final_count})")
        
        return cleaned_df
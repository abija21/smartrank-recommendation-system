import json
import logging
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, size, concat_ws
from pyspark.sql.types import StringType, FloatType, IntegerType, LongType, BooleanType

from .config import PipelineConfig

logger = logging.getLogger(__name__)

class DataLoader:
    """Handles loading and basic preprocessing of raw data"""
    
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
    
    def load_reviews(self, file_path: str) -> DataFrame:
        """Load and parse reviews JSONL file"""
        logger.info(f"Loading reviews from {file_path}")
        
        reviews_df = self.spark.read.json(file_path)
        
        # Select and clean relevant columns
        cleaned_df = reviews_df.select(
            when(col("rating").isNotNull(), col("rating")).otherwise(0.0).cast(FloatType()).alias("rating"),
            when(col("title").isNotNull(), col("title")).otherwise("").cast(StringType()).alias("title"),
            when(col("text").isNotNull(), col("text")).otherwise("").cast(StringType()).alias("text"),
            col("asin").cast(StringType()),
            col("parent_asin").cast(StringType()),
            col("user_id").cast(StringType()),
            when(col("timestamp").isNotNull(), col("timestamp")).otherwise(0).cast(LongType()).alias("timestamp"),
            when(col("verified_purchase").isNotNull(), col("verified_purchase")).otherwise(False).cast(BooleanType()).alias("verified_purchase"),
            when(col("helpful_vote").isNotNull(), col("helpful_vote")).otherwise(0).cast(IntegerType()).alias("helpful_vote")
        )
        
        logger.info(f"Loaded {cleaned_df.count()} reviews")
        return cleaned_df
    
    def load_metadata(self, file_path: str) -> DataFrame:
        """Load and parse metadata JSONL file"""
        logger.info(f"Loading metadata from {file_path}")
        
        # Use manual parsing to avoid column conflicts
        raw_df = self.spark.read.text(file_path)
        
        def parse_metadata_record(row):
            try:
                data = json.loads(row.value)
                return {
                    'main_category': data.get('main_category', 'Unknown'),
                    'title': data.get('title', 'Unknown Product'),
                    'average_rating': float(data.get('average_rating', 0.0)) if data.get('average_rating') is not None else 0.0,
                    'rating_number': int(data.get('rating_number', 0)) if data.get('rating_number') is not None else 0,
                    'price': float(data.get('price', 0.0)) if data.get('price') is not None else 0.0,
                    'store': data.get('store', 'Unknown Store'),
                    'parent_asin': data.get('parent_asin'),
                    'features': data.get('features', []) if data.get('features') else [],
                    'description': data.get('description', []) if data.get('description') else []
                }
            except Exception as e:
                logger.debug(f"Failed to parse metadata record: {e}")
                return None
        
        parsed_rdd = raw_df.rdd.map(parse_metadata_record).filter(lambda x: x is not None)
        metadata_df = parsed_rdd.toDF()
        
        # Process and clean columns
        cleaned_df = metadata_df.select(
            col("main_category").cast(StringType()),
            col("title").cast(StringType()),
            col("average_rating").cast(FloatType()),
            col("rating_number").cast(IntegerType()),
            col("price").cast(FloatType()),
            col("store").cast(StringType()),
            col("parent_asin").cast(StringType()),
            when(col("features").isNotNull() & (size(col("features")) > 0), 
                 concat_ws(" ", col("features"))).otherwise("").alias("features_text"),
            when(col("description").isNotNull() & (size(col("description")) > 0),
                 concat_ws(" ", col("description"))).otherwise("").alias("description_text")
        )
        
        logger.info(f"Loaded {cleaned_df.count()} product metadata records")
        return cleaned_df
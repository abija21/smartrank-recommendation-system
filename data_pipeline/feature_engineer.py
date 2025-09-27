import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, avg, count, concat_ws
from pyspark.ml.feature import Tokenizer, HashingTF, IDF

from .config import PipelineConfig

logger = logging.getLogger(__name__)

class FeatureEngineer:
    """Creates features for machine learning models"""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
    
    def create_product_features(self, reviews_df: DataFrame, metadata_df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """Create comprehensive product and user features"""
        
        # Aggregate review statistics per product
        review_stats = self._create_review_aggregations(reviews_df)
        
        # Create user statistics
        user_stats = self._create_user_aggregations(reviews_df)
        
        # Create text features
        text_features_df = self._create_text_features(metadata_df)
        
        # Combine all features
        final_features = self._combine_features(metadata_df, review_stats, text_features_df)
        
        logger.info(f"Created features for {final_features.count()} products")
        return final_features, user_stats
    
    def _create_review_aggregations(self, reviews_df: DataFrame) -> DataFrame:
        """Aggregate review statistics per product"""
        return reviews_df.groupBy("parent_asin").agg(
            avg("rating").alias("avg_user_rating"),
            count("rating").alias("review_count"),
            avg("helpful_vote").alias("avg_helpful_votes"),
            avg(when(col("timestamp") > 1600000000000, 1.0).otherwise(0.5)).alias("recency_score")
        )
    
    def _create_user_aggregations(self, reviews_df: DataFrame) -> DataFrame:
        """Create user interaction features"""
        return reviews_df.groupBy("user_id").agg(
            count("rating").alias("user_review_count"),
            avg("rating").alias("user_avg_rating")
        )
    
    def _create_text_features(self, metadata_df: DataFrame) -> DataFrame:
        """Create TF-IDF features from product text"""
        # Combine text fields
        content_df = metadata_df.select(
            "parent_asin",
            concat_ws(" ", col("title"), col("features_text"), col("description_text")).alias("combined_text")
        )
        
        # Tokenize
        tokenizer = Tokenizer(inputCol="combined_text", outputCol="words")
        tokenized_df = tokenizer.transform(content_df)
        
        # TF-IDF
        hashingTF = HashingTF(inputCol="words", outputCol="raw_features", numFeatures=self.config.tfidf_num_features)
        tf_df = hashingTF.transform(tokenized_df)
        
        idf = IDF(inputCol="raw_features", outputCol="tfidf_features")
        idf_model = idf.fit(tf_df)
        
        return idf_model.transform(tf_df)
    
    def _combine_features(self, metadata_df: DataFrame, review_stats: DataFrame, text_features_df: DataFrame) -> DataFrame:
        """Combine all features into final feature set"""
        # Join all features
        features_df = metadata_df.select(
            "parent_asin", "title", "main_category", "store", 
            "average_rating", "rating_number", "price_clean"
        ).join(
            review_stats, on="parent_asin", how="left"
        ).join(
            text_features_df.select("parent_asin", "tfidf_features"), 
            on="parent_asin", how="left"
        )
        
        # Fill nulls and add derived features
        return features_df.fillna({
            "avg_user_rating": 0.0,
            "review_count": 0,
            "avg_helpful_votes": 0.0,
            "recency_score": 0.0
        }).withColumn(
            "popularity_score",
            col("review_count") * col("avg_user_rating") * 0.01
        )
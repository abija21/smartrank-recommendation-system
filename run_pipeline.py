import sys
import logging
from pathlib import Path

# Add the project root to Python path
sys.path.append(str(Path(__file__).parent))

from data_pipeline import SmartRankPipeline, PipelineConfig

def main():
    """Run the SmartRank data pipeline"""
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s: %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    
    print("Starting SmartRank Data Pipeline")
    print("-" * 50)
    
    try:
        # Initialize pipeline with default config
        config = PipelineConfig()
        pipeline = SmartRankPipeline(config)
        
        # Run pipeline
        success = pipeline.run()
        
        if success:
            print("\n Pipeline completed successfully!")
            print(f" Processed data saved to: {config.processed_data_path}")
            return 0
        else:
            print("\n Pipeline failed!")
            return 1
            
    except KeyboardInterrupt:
        logger.warning("\nPipeline interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
import logging
from .pipeline import SmartRankPipeline
from .config import PipelineConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    """Main entry point"""
    config = PipelineConfig()
    pipeline = SmartRankPipeline(config)
    
    success = pipeline.run()
    exit(0 if success else 1)

if __name__ == "__main__":
    main()
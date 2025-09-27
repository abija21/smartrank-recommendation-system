import os
from pathlib import Path

def ensure_directories(*paths: str) -> None:
    """Create directories if they don't exist"""
    for path in paths:
        Path(path).mkdir(parents=True, exist_ok=True)

def get_file_size_mb(file_path: str) -> float:
    """Get file size in MB"""
    if os.path.exists(file_path):
        return os.path.getsize(file_path) / (1024 * 1024)
    return 0.0

def validate_data_files(data_dir: str) -> bool:
    """Validate that required data files exist"""
    required_files = ["All_Beauty.jsonl", "meta_All_Beauty.jsonl"]
    
    for file_name in required_files:
        file_path = os.path.join(data_dir, file_name)
        if not os.path.exists(file_path):
            print(f" Missing required file: {file_path}")
            return False
        
        size_mb = get_file_size_mb(file_path)
        print(f" Found {file_name} ({size_mb:.1f} MB)")
    
    return True
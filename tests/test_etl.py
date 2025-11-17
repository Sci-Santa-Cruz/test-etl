import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from modules.transformation import DataTransformer
from modules.loading import MySQLLoader
import logging

logging.basicConfig(level=logging.INFO)

def test_transformation():
    """Test transformation module"""
    transformer = DataTransformer()
    result = transformer.transform_file('data/raw/report_7.txt')

    print(f"Valid records: {len(result['valid'])}")
    print(f"Error records: {len(result['errors'])}")
    print(f"GE validation success: {result['ge_results']['success']}")

    return result

def test_loading():
    """Test loading module (requires MySQL)"""
    # This would require a test database
    pass

if __name__ == "__main__":
    print("Testing ETL transformation...")
    result = test_transformation()
    print("Test completed.")
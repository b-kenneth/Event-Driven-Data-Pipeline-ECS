import boto3
import pandas as pd
import json
import logging
from datetime import datetime
from typing import Dict, Any, Iterator
from io import StringIO

def setup_logging():
    """Setup structured logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('/app/logs/validation.log')
        ]
    )
    return logging.getLogger(__name__)

class S3Handler:
    def __init__(self):
        self.s3_client = boto3.client('s3')
    
    def read_csv_sample(self, bucket: str, key: str, nrows: int = 5) -> pd.DataFrame:
        """Read a small sample of CSV for structure validation"""
        response = self.s3_client.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(response['Body'], nrows=nrows)
    
    def read_csv(self, bucket: str, key: str) -> pd.DataFrame:
        """Read entire CSV file"""
        response = self.s3_client.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(response['Body'])
    
    def read_csv_chunks(self, bucket: str, key: str, chunksize: int = 10000) -> Iterator[pd.DataFrame]:
        """Read CSV in chunks for memory efficiency"""
        response = self.s3_client.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(response['Body'], chunksize=chunksize)
    
    def upload_json(self, bucket: str, key: str, data: Dict[str, Any]):
        """Upload JSON data to S3"""
        json_data = json.dumps(data, indent=2, default=str)
        self.s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json_data,
            ContentType='application/json'
        )

class ValidationReport:
    def __init__(self, batch_id: str):
        self.batch_id = batch_id
        self.start_time = datetime.utcnow()
        self.end_time = None
        self.overall_status = None
        self.dataset_results = {}
        self.summary = {}
    
    def add_dataset_result(self, dataset: str, result: Dict):
        """Add validation result for a dataset"""
        self.dataset_results[dataset] = result
    
    def set_overall_status(self, success: bool):
        """Set overall validation status"""
        self.overall_status = "SUCCESS" if success else "FAILED"
        self.end_time = datetime.utcnow()
    
    def add_summary(self, validation_results: Dict):
        """Add summary statistics"""
        total_records = sum(result.get('total_records', 0) for result in validation_results.values())
        total_errors = sum(result.get('total_errors', 0) for result in validation_results.values())
        
        self.summary = {
            'total_records_processed': total_records,
            'total_errors_found': total_errors,
            'error_rate': (total_errors / total_records * 100) if total_records > 0 else 0,
            'processing_duration_seconds': (self.end_time - self.start_time).total_seconds()
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert report to dictionary"""
        return {
            'batch_id': self.batch_id,
            'validation_timestamp': self.start_time.isoformat(),
            'completion_timestamp': self.end_time.isoformat() if self.end_time else None,
            'overall_status': self.overall_status,
            'summary': self.summary,
            'dataset_results': self.dataset_results
        }

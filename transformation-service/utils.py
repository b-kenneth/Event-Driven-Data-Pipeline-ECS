import boto3
import json
import logging
import os
from datetime import datetime
from typing import Dict, Any

def setup_logging():
    """Setup structured logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('/app/logs/transformation.log')
        ]
    )
    return logging.getLogger(__name__)

class S3Handler:
    def __init__(self):
        self.s3_client = boto3.client('s3')
    
    def upload_json(self, bucket: str, key: str, data: Dict[str, Any]):
        """Upload JSON data to S3"""
        json_data = json.dumps(data, indent=2, default=str)
        self.s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json_data,
            ContentType='application/json'
        )

class TransformationReport:
    def __init__(self, batch_id: str):
        self.batch_id = batch_id
        self.start_time = datetime.utcnow()
        self.end_time = None
        self.status = None
        self.category_kpi_count = 0
        self.order_kpi_count = 0
    
    def set_kpi_counts(self, category_count: int, order_count: int):
        """Set KPI counts"""
        self.category_kpi_count = category_count
        self.order_kpi_count = order_count
    
    def set_status(self, success: bool):
        """Set transformation status"""
        self.status = "SUCCESS" if success else "FAILED"
        self.end_time = datetime.utcnow()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert report to dictionary"""
        return {
            'batch_id': self.batch_id,
            'transformation_timestamp': self.start_time.isoformat(),
            'completion_timestamp': self.end_time.isoformat() if self.end_time else None,
            'status': self.status,
            'kpi_counts': {
                'category_kpis': self.category_kpi_count,
                'order_kpis': self.order_kpi_count,
                'total_kpis': self.category_kpi_count + self.order_kpi_count
            },
            'processing_duration_seconds': (self.end_time - self.start_time).total_seconds() if self.end_time else None
        }

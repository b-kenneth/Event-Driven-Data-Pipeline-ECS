import pytest
import json
import boto3
from moto import mock_s3, mock_dynamodb
from datetime import datetime

from lambda_functions.file_archiver.lambda_function import (
    lambda_handler,
    archive_successful_files,
    archive_failed_files,
    update_processed_files_status
)

@mock_s3
@mock_dynamodb
class TestFileArchiverLambda:
    
    def setup_method(self):
        """Set up test environment"""
        self.s3_client = boto3.client('s3', region_name='us-east-1')
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        
        # Create test bucket
        self.bucket_name = 'test-ecommerce-bucket'
        self.s3_client.create_bucket(Bucket=self.bucket_name)
        
        # Create ProcessedFiles table
        self.dynamodb.create_table(
            TableName='ProcessedFiles',
            KeySchema=[
                {'AttributeName': 'file_key', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'file_key', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        
        # Set environment variables
        import os
        os.environ['S3_BUCKET_NAME'] = self.bucket_name
    
    def test_successful_file_archival(self):
        """Test successful file archival to processed directory"""
        # Upload test files
        test_files = [
            'raw-data/products/product1.csv',
            'raw-data/orders/order1.csv',
            'raw-data/order-items/item1.csv'
        ]
        
        for file_key in test_files:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=file_key,
                Body='test,data\n1,value'
            )
        
        # Create archiver event
        event = {
            'batch_id': 'test-batch-001',
            'status': 'SUCCESS',
            'products_files': ['raw-data/products/product1.csv'],
            'orders_files': ['raw-data/orders/order1.csv'],
            'order_items_files': ['raw-data/order-items/item1.csv']
        }
        
        # Execute Lambda
        result = lambda_handler(event, {})
        
        # Assertions
        assert result['statusCode'] == 200
        assert 'Successfully archived 3 files' in result['body']
        
        # Verify files were moved to processed directory
        today = datetime.now().strftime('%Y/%m/%d')
        processed_objects = self.s3_client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=f'processed/{today}/'
        )
        
        assert 'Contents' in processed_objects
        assert len(processed_objects['Contents']) == 3
        
        # Verify original files were deleted
        raw_objects = self.s3_client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix='raw-data/'
        )
        
        assert 'Contents' not in raw_objects or len(raw_objects.get('Contents', [])) == 0
    
    def test_failed_file_archival(self):
        """Test failed file archival to failed directory"""
        # Upload test files
        test_files = ['raw-data/products/product1.csv']
        
        for file_key in test_files:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=file_key,
                Body='test,data\n1,value'
            )
        
        # Create archiver event with error
        event = {
            'batch_id': 'test-batch-002',
            'status': 'FAILED',
            'products_files': ['raw-data/products/product1.csv'],
            'orders_files': [],
            'order_items_files': [],
            'error': {
                'Error': 'ValidationError',
                'Cause': 'Invalid data format'
            }
        }
        
        # Execute Lambda
        result = lambda_handler(event, {})
        
        # Assertions
        assert result['statusCode'] == 200
        
        # Verify files were moved to failed directory
        today = datetime.now().strftime('%Y/%m/%d')
        failed_objects = self.s3_client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=f'failed/{today}/'
        )
        
        assert 'Contents' in failed_objects
        
        # Verify error log was created
        error_log_objects = self.s3_client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=f'failed/{today}/error-logs/'
        )
        
        assert 'Contents' in error_log_objects
    
    def test_dynamodb_status_update(self):
        """Test DynamoDB status updates"""
        event = {
            'batch_id': 'test-batch-003',
            'status': 'SUCCESS',
            'products_files': ['raw-data/products/product1.csv'],
            'orders_files': [],
            'order_items_files': []
        }
        
        # Execute Lambda
        result = lambda_handler(event, {})
        
        # Verify DynamoDB was updated
        processed_files_table = self.dynamodb.Table('ProcessedFiles')
        response = processed_files_table.get_item(
            Key={'file_key': 'raw-data/products/product1.csv'}
        )
        
        assert 'Item' in response
        item = response['Item']
        assert item['processing_status'] == 'SUCCESS'
        assert item['batch_id'] == 'test-batch-003'
        assert 'processed_at' in item

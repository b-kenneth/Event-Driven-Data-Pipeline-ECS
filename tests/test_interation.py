import pytest
import json
import boto3
from moto import mock_s3, mock_dynamodb, mock_stepfunctions, mock_ecs
from unittest.mock import patch

class TestPipelineIntegration:
    
    @mock_s3
    @mock_dynamodb
    @mock_stepfunctions
    def test_end_to_end_pipeline_success(self):
        """Test complete pipeline from file upload to KPI storage"""
        # Set up AWS resources
        s3_client = boto3.client('s3', region_name='us-east-1')
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        stepfunctions = boto3.client('stepfunctions', region_name='us-east-1')
        
        # Create S3 bucket
        bucket_name = 'test-ecommerce-bucket'
        s3_client.create_bucket(Bucket=bucket_name)
        
        # Create DynamoDB tables
        self.create_test_dynamodb_tables(dynamodb)
        
        # Upload test files
        self.upload_test_files(s3_client, bucket_name)
        
        # Simulate file detector Lambda
        with patch('lambda_functions.file_detector.lambda_function.trigger_pipeline') as mock_trigger:
            from lambda_functions.file_detector.lambda_function import lambda_handler
            
            event = {
                'Records': [{
                    's3': {
                        'bucket': {'name': bucket_name},
                        'object': {'key': 'raw-data/products/product1.csv'}
                    }
                }]
            }
            
            result = lambda_handler(event, {})
            
            # Verify pipeline was triggered
            assert result['statusCode'] == 200
            mock_trigger.assert_called_once()
    
    def create_test_dynamodb_tables(self, dynamodb):
        """Create test DynamoDB tables"""
        # ProcessedFiles table
        dynamodb.create_table(
            TableName='ProcessedFiles',
            KeySchema=[{'AttributeName': 'file_key', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'file_key', 'AttributeType': 'S'}],
            BillingMode='PAY_PER_REQUEST'
        )
        
        # PendingBatches table
        dynamodb.create_table(
            TableName='PendingBatches',
            KeySchema=[{'AttributeName': 'bucket_name', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'bucket_name', 'AttributeType': 'S'}],
            BillingMode='PAY_PER_REQUEST'
        )
        
        # CategoryKPIs table
        dynamodb.create_table(
            TableName='CategoryKPIs',
            KeySchema=[
                {'AttributeName': 'category', 'KeyType': 'HASH'},
                {'AttributeName': 'order_date', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'category', 'AttributeType': 'S'},
                {'AttributeName': 'order_date', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        
        # OrderKPIs table
        dynamodb.create_table(
            TableName='OrderKPIs',
            KeySchema=[{'AttributeName': 'order_date', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'order_date', 'AttributeType': 'S'}],
            BillingMode='PAY_PER_REQUEST'
        )
    
    def upload_test_files(self, s3_client, bucket_name):
        """Upload test files to S3"""
        # Products file
        products_content = """id,sku,cost,category,name,brand,retail_price,department
1,ABC-12345678,10.50,Electronics,Product 1,Brand A,20.00,Tech
2,DEF-87654321,25.00,Clothing,Product 2,Brand B,50.00,Fashion
3,GHI-11111111,15.75,Books,Product 3,Brand C,30.00,Media"""
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key='raw-data/products/product1.csv',
            Body=products_content
        )
        
        # Orders file
        orders_content = """order_id,user_id,status,created_at,num_of_item
1,100,delivered,2025-01-01T10:00:00,2
2,101,returned,2025-01-01T11:00:00,1
3,102,delivered,2025-01-02T10:00:00,3"""
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key='raw-data/orders/order1.csv',
            Body=orders_content
        )
        
        # Order items file
        order_items_content = """id,order_id,user_id,product_id,status,created_at,sale_price
1,1,100,1,delivered,2025-01-01T10:00:00,18.00
2,1,100,2,delivered,2025-01-01T10:00:00,45.00
3,2,101,3,returned,2025-01-01T11:00:00,28.00
4,3,102,1,delivered,2025-01-02T10:00:00,18.00
5,3,102,2,delivered,2025-01-02T10:00:00,45.00
6,3,102,3,delivered,2025-01-02T10:00:00,28.00"""
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key='raw-data/order-items/item1.csv',
            Body=order_items_content
        )

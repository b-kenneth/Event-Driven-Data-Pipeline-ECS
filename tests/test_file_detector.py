import pytest
import json
import boto3
from moto import mock_s3, mock_dynamodb, mock_stepfunctions, mock_events
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

# Import your Lambda function
from lambda_functions.file_detector.lambda_function import (
    lambda_handler,
    check_batch_status,
    is_file_already_processed,
    schedule_delayed_trigger,
    trigger_pipeline
)

@mock_s3
@mock_dynamodb
@mock_stepfunctions
@mock_events
class TestFileDetectorLambda:
    
    def setup_method(self):
        """Set up test environment before each test"""
        # Create mock AWS resources
        self.s3_client = boto3.client('s3', region_name='us-east-1')
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        self.stepfunctions = boto3.client('stepfunctions', region_name='us-east-1')
        
        # Create test bucket
        self.bucket_name = 'test-ecommerce-bucket'
        self.s3_client.create_bucket(Bucket=self.bucket_name)
        
        # Create DynamoDB tables
        self.create_test_tables()
        
        # Set environment variables
        import os
        os.environ['STEP_FUNCTIONS_ARN'] = 'arn:aws:states:us-east-1:123456789012:stateMachine:test-pipeline'
    
    def create_test_tables(self):
        """Create test DynamoDB tables"""
        # ProcessedFiles table
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
        
        # PendingBatches table
        self.dynamodb.create_table(
            TableName='PendingBatches',
            KeySchema=[
                {'AttributeName': 'bucket_name', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'bucket_name', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )
    
    def upload_test_files(self, file_configs):
        """Upload test files to S3"""
        for config in file_configs:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=config['key'],
                Body=config['content']
            )
    
    def test_immediate_trigger_with_10_files(self):
        """Test immediate pipeline trigger when 10+ files are present"""
        # Upload 10+ files (4 products, 3 orders, 4 order_items)
        test_files = [
            {'key': 'raw-data/products/product1.csv', 'content': 'id,name\n1,Product1'},
            {'key': 'raw-data/products/product2.csv', 'content': 'id,name\n2,Product2'},
            {'key': 'raw-data/products/product3.csv', 'content': 'id,name\n3,Product3'},
            {'key': 'raw-data/products/product4.csv', 'content': 'id,name\n4,Product4'},
            {'key': 'raw-data/orders/order1.csv', 'content': 'order_id,user_id\n1,100'},
            {'key': 'raw-data/orders/order2.csv', 'content': 'order_id,user_id\n2,101'},
            {'key': 'raw-data/orders/order3.csv', 'content': 'order_id,user_id\n3,102'},
            {'key': 'raw-data/order-items/item1.csv', 'content': 'id,order_id\n1,1'},
            {'key': 'raw-data/order-items/item2.csv', 'content': 'id,order_id\n2,2'},
            {'key': 'raw-data/order-items/item3.csv', 'content': 'id,order_id\n3,3'},
            {'key': 'raw-data/order-items/item4.csv', 'content': 'id,order_id\n4,4'},
        ]
        
        self.upload_test_files(test_files)
        
        # Create S3 event
        event = {
            'Records': [{
                's3': {
                    'bucket': {'name': self.bucket_name},
                    'object': {'key': 'raw-data/products/product4.csv'}
                }
            }]
        }
        
        # Mock Step Functions execution
        with patch('boto3.client') as mock_boto3:
            mock_stepfunctions = MagicMock()
            mock_boto3.return_value = mock_stepfunctions
            mock_stepfunctions.start_execution.return_value = {
                'executionArn': 'arn:aws:states:us-east-1:123456789012:execution:test'
            }
            
            # Execute Lambda
            result = lambda_handler(event, {})
            
            # Assertions
            assert result['statusCode'] == 200
            mock_stepfunctions.start_execution.assert_called_once()
            
            # Verify Step Functions was called with correct input
            call_args = mock_stepfunctions.start_execution.call_args
            input_data = json.loads(call_args[1]['input'])
            
            assert len(input_data['products_files']) == 4
            assert len(input_data['orders_files']) == 3
            assert len(input_data['order_items_files']) == 4
    
    def test_delayed_trigger_with_minimum_files(self):
        """Test 20-minute timer setup when minimum files present but < 10 total"""
        # Upload minimum files (1 of each type = 3 total)
        test_files = [
            {'key': 'raw-data/products/product1.csv', 'content': 'id,name\n1,Product1'},
            {'key': 'raw-data/orders/order1.csv', 'content': 'order_id,user_id\n1,100'},
            {'key': 'raw-data/order-items/item1.csv', 'content': 'id,order_id\n1,1'},
        ]
        
        self.upload_test_files(test_files)
        
        # Create S3 event
        event = {
            'Records': [{
                's3': {
                    'bucket': {'name': self.bucket_name},
                    'object': {'key': 'raw-data/products/product1.csv'}
                }
            }]
        }
        
        # Mock EventBridge and context
        context = MagicMock()
        context.invoked_function_arn = 'arn:aws:lambda:us-east-1:123456789012:function:test'
        
        with patch('boto3.client') as mock_boto3:
            mock_events = MagicMock()
            mock_boto3.return_value = mock_events
            
            # Execute Lambda
            result = lambda_handler(event, context)
            
            # Assertions
            assert result['statusCode'] == 200
            mock_events.put_rule.assert_called_once()
            mock_events.put_targets.assert_called_once()
            
            # Verify EventBridge rule creation
            rule_call = mock_events.put_rule.call_args
            assert 'delayed-trigger' in rule_call[1]['Name']
            assert rule_call[1]['ScheduleExpression'] == 'rate(20 minutes)'
    
    def test_skip_already_processed_files(self):
        """Test that already processed files are skipped"""
        # Mark a file as already processed
        processed_files_table = self.dynamodb.Table('ProcessedFiles')
        processed_files_table.put_item(
            Item={
                'file_key': 'raw-data/products/product1.csv',
                'processing_status': 'SUCCESS',
                'processed_at': datetime.now().isoformat()
            }
        )
        
        # Create S3 event for the processed file
        event = {
            'Records': [{
                's3': {
                    'bucket': {'name': self.bucket_name},
                    'object': {'key': 'raw-data/products/product1.csv'}
                }
            }]
        }
        
        # Mock S3 operations
        with patch('boto3.client') as mock_boto3:
            mock_s3 = MagicMock()
            mock_boto3.return_value = mock_s3
            
            # Execute Lambda
            result = lambda_handler(event, {})
            
            # Verify file was moved to archive
            mock_s3.copy_object.assert_called_once()
            mock_s3.delete_object.assert_called_once()
    
    def test_incomplete_batch_no_trigger(self):
        """Test that incomplete batches don't trigger pipeline"""
        # Upload only products files (missing orders and order_items)
        test_files = [
            {'key': 'raw-data/products/product1.csv', 'content': 'id,name\n1,Product1'},
            {'key': 'raw-data/products/product2.csv', 'content': 'id,name\n2,Product2'},
        ]
        
        self.upload_test_files(test_files)
        
        # Create S3 event
        event = {
            'Records': [{
                's3': {
                    'bucket': {'name': self.bucket_name},
                    'object': {'key': 'raw-data/products/product1.csv'}
                }
            }]
        }
        
        # Execute Lambda
        result = lambda_handler(event, {})
        
        # Verify no pipeline trigger
        assert result['statusCode'] == 200
        # No Step Functions or EventBridge calls should be made

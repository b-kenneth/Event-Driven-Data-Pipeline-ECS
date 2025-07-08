import pytest
import pandas as pd
import boto3
from moto import mock_dynamodb
from decimal import Decimal
from dynamodb_handler import DynamoDBHandler

@mock_dynamodb
class TestDynamoDBHandler:
    
    def setup_method(self):
        """Set up test environment"""
        # Set environment variables
        import os
        os.environ['CATEGORY_KPIS_TABLE'] = 'TestCategoryKPIs'
        os.environ['ORDER_KPIS_TABLE'] = 'TestOrderKPIs'
        os.environ['BATCH_ID'] = 'test-batch-001'
        
        # Create mock DynamoDB tables
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        
        # Create CategoryKPIs table
        self.dynamodb.create_table(
            TableName='TestCategoryKPIs',
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
        
        # Create OrderKPIs table
        self.dynamodb.create_table(
            TableName='TestOrderKPIs',
            KeySchema=[
                {'AttributeName': 'order_date', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'order_date', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        
        self.handler = DynamoDBHandler()
    
    def test_store_category_kpis_success(self):
        """Test successful category KPIs storage"""
        # Create test data
        category_kpis_df = pd.DataFrame({
            'category': ['Electronics', 'Clothing'],
            'order_date': ['2025-01-01', '2025-01-01'],
            'daily_revenue': [1000.50, 750.25],
            'avg_order_value': [100.05, 75.03],
            'avg_return_rate': [0.05, 0.10]
        })
        
        # Store KPIs
        result = self.handler.store_category_kpis(category_kpis_df)
        
        # Assertions
        assert result == True
        
        # Verify data was stored
        table = self.dynamodb.Table('TestCategoryKPIs')
        response = table.scan()
        
        assert len(response['Items']) == 2
        
        # Check specific item
        electronics_item = table.get_item(
            Key={'category': 'Electronics', 'order_date': '2025-01-01'}
        )['Item']
        
        assert electronics_item['daily_revenue'] == Decimal('1000.50')
        assert electronics_item['avg_order_value'] == Decimal('100.05')
        assert electronics_item['avg_return_rate'] == Decimal('0.05')
        assert electronics_item['batch_id'] == 'test-batch-001'
        assert 'updated_at' in electronics_item
    
    def test_store_order_kpis_success(self):
        """Test successful order KPIs storage"""
        # Create test data
        order_kpis_df = pd.DataFrame({
            'order_date': ['2025-01-01', '2025-01-02'],
            'total_orders': [150, 200],
            'total_revenue': [15000.75, 20000.50],
            'total_items_sold': [300, 450],
            'unique_customers': [120, 180],
            'return_rate': [0.08, 0.06]
        })
        
        # Store KPIs
        result = self.handler.store_order_kpis(order_kpis_df)
        
        # Assertions
        assert result == True
        
        # Verify data was stored
        table = self.dynamodb.Table('TestOrderKPIs')
        response = table.scan()
        
        assert len(response['Items']) == 2
        
        # Check specific item
        jan_1_item = table.get_item(
            Key={'order_date': '2025-01-01'}
        )['Item']
        
        assert jan_1_item['total_orders'] == 150
        assert jan_1_item['total_revenue'] == Decimal('15000.75')
        assert jan_1_item['total_items_sold'] == 300
        assert jan_1_item['unique_customers'] == 120
        assert jan_1_item['return_rate'] == Decimal('0.08')
        assert jan_1_item['batch_id'] == 'test-batch-001'
    
    def test_store_empty_dataframe(self):
        """Test handling of empty DataFrames"""
        empty_df = pd.DataFrame(columns=['category', 'order_date', 'daily_revenue', 'avg_order_value', 'avg_return_rate'])
        
        result = self.handler.store_category_kpis(empty_df)
        
        # Should still return True but store no items
        assert result == True
        
        table = self.dynamodb.Table('TestCategoryKPIs')
        response = table.scan()
        
        assert len(response['Items']) == 0

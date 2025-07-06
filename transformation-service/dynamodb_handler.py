import boto3
import pandas as pd
from decimal import Decimal
from datetime import datetime
import logging
import os
from typing import Dict, List, Any

class DynamoDBHandler:
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb')
        self.logger = logging.getLogger(__name__)
        
        # Table names from environment variables
        self.category_table_name = os.environ.get('CATEGORY_KPIS_TABLE', 'CategoryKPIs')
        self.order_table_name = os.environ.get('ORDER_KPIS_TABLE', 'OrderKPIs')
        
        # Get table references
        self.category_table = self.dynamodb.Table(self.category_table_name)
        self.order_table = self.dynamodb.Table(self.order_table_name)
    
    def store_category_kpis(self, category_kpis_df: pd.DataFrame) -> bool:
        """Store Category-Level KPIs in DynamoDB"""
        try:
            self.logger.info(f"Storing {len(category_kpis_df)} category KPI records")
            
            # Convert DataFrame to DynamoDB items
            items = []
            for _, row in category_kpis_df.iterrows():
                item = {
                    'category': str(row['category']),
                    'order_date': str(row['order_date']),
                    'daily_revenue': Decimal(str(row['daily_revenue'])),
                    'avg_order_value': Decimal(str(row['avg_order_value'])),
                    'avg_return_rate': Decimal(str(row['avg_return_rate'])),
                    'updated_at': datetime.utcnow().isoformat(),
                    'batch_id': os.environ.get('BATCH_ID', 'unknown')
                }
                items.append(item)
            
            # Batch write to DynamoDB
            self._batch_write_items(self.category_table, items)
            
            self.logger.info("✅ Category KPIs stored successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to store category KPIs: {str(e)}")
            return False
    
    def store_order_kpis(self, order_kpis_df: pd.DataFrame) -> bool:
        """Store Order-Level KPIs in DynamoDB"""
        try:
            self.logger.info(f"Storing {len(order_kpis_df)} order KPI records")
            
            # Convert DataFrame to DynamoDB items
            items = []
            for _, row in order_kpis_df.iterrows():
                item = {
                    'order_date': str(row['order_date']),
                    'total_orders': int(row['total_orders']),
                    'total_revenue': Decimal(str(row['total_revenue'])),
                    'total_items_sold': int(row['total_items_sold']),
                    'unique_customers': int(row['unique_customers']),
                    'return_rate': Decimal(str(row['return_rate'])),
                    'updated_at': datetime.utcnow().isoformat(),
                    'batch_id': os.environ.get('BATCH_ID', 'unknown')
                }
                items.append(item)
            
            # Batch write to DynamoDB
            self._batch_write_items(self.order_table, items)
            
            self.logger.info("✅ Order KPIs stored successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to store order KPIs: {str(e)}")
            return False
    
    def _batch_write_items(self, table, items: List[Dict[str, Any]]):
        """Efficiently batch write items to DynamoDB"""
        
        # DynamoDB batch_writer handles batching automatically
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)
        
        self.logger.info(f"Batch wrote {len(items)} items to {table.name}")

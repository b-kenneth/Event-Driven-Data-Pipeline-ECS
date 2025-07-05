import pandas as pd
import numpy as np
from datetime import datetime
import re
from typing import Dict, List, Any

class ValidationRules:
    def __init__(self):
        self.required_headers = {
            'products': ['id', 'sku', 'cost', 'category', 'name', 'brand', 'retail_price', 'department'],
            'orders': ['order_id', 'user_id', 'status', 'created_at', 'num_of_item'],
            'order_items': ['id', 'order_id', 'user_id', 'product_id', 'status', 'created_at', 'sale_price']
        }
        
        self.valid_order_statuses = {'delivered', 'returned', 'shipped', 'cancelled', 'pending'}
        self.valid_item_statuses = {'delivered', 'returned', 'shipped', 'cancelled', 'pending'}
    
    def validate_headers(self, file_type: str, headers: List[str]) -> bool:
        """Validate that all required headers are present"""
        required = set(self.required_headers[file_type])
        actual = set(headers)
        
        missing = required - actual
        if missing:
            print(f"Missing required headers for {file_type}: {missing}")
            return False
        
        return True
    
    def validate_products_chunk(self, chunk: pd.DataFrame, chunk_num: int) -> Dict:
        """Validate products data chunk"""
        errors = {
            'null_ids': [],
            'invalid_prices': [],
            'negative_costs': [],
            'invalid_retail_prices': [],
            'cost_greater_than_retail': [],
            'empty_names': [],
            'invalid_skus': []
        }
        
        # Check for null IDs
        null_id_mask = chunk['id'].isnull()
        if null_id_mask.any():
            errors['null_ids'] = chunk[null_id_mask].index.tolist()
        
        # Validate prices
        try:
            chunk['cost'] = pd.to_numeric(chunk['cost'], errors='coerce')
            chunk['retail_price'] = pd.to_numeric(chunk['retail_price'], errors='coerce')
            
            # Negative costs
            negative_cost_mask = chunk['cost'] < 0
            if negative_cost_mask.any():
                errors['negative_costs'] = chunk[negative_cost_mask]['id'].tolist()
            
            # Invalid retail prices
            invalid_retail_mask = chunk['retail_price'] <= 0
            if invalid_retail_mask.any():
                errors['invalid_retail_prices'] = chunk[invalid_retail_mask]['id'].tolist()
            
            # Cost greater than retail price
            cost_gt_retail_mask = chunk['cost'] > chunk['retail_price']
            if cost_gt_retail_mask.any():
                errors['cost_greater_than_retail'] = chunk[cost_gt_retail_mask]['id'].tolist()
                
        except Exception as e:
            errors['invalid_prices'] = [f"Price conversion error: {str(e)}"]
        
        # Validate names
        empty_name_mask = chunk['name'].isnull() | (chunk['name'].str.strip() == '')
        if empty_name_mask.any():
            errors['empty_names'] = chunk[empty_name_mask]['id'].tolist()
        
        # Validate SKU format (basic pattern check)
        invalid_sku_mask = ~chunk['sku'].str.match(r'^[a-zA-Z]{3}-\d{8}$', na=False)
        if invalid_sku_mask.any():
            errors['invalid_skus'] = chunk[invalid_sku_mask]['id'].tolist()
        
        total_errors = sum(len(error_list) for error_list in errors.values())
        
        return {
            'chunk_num': chunk_num,
            'total_errors': total_errors,
            'error_details': {k: v for k, v in errors.items() if v}
        }
    
    def validate_orders_chunk(self, chunk: pd.DataFrame, chunk_num: int, reference_data: Dict) -> Dict:
        """Validate orders data chunk"""
        errors = {
            'null_order_ids': [],
            'null_user_ids': [],
            'invalid_statuses': [],
            'invalid_dates': [],
            'invalid_num_items': [],
            'date_logic_errors': []
        }
        
        # Check for null order IDs
        null_order_mask = chunk['order_id'].isnull()
        if null_order_mask.any():
            errors['null_order_ids'] = chunk[null_order_mask].index.tolist()
        
        # Check for null user IDs
        null_user_mask = chunk['user_id'].isnull()
        if null_user_mask.any():
            errors['null_user_ids'] = chunk[null_user_mask].index.tolist()
        
        # Validate order statuses
        invalid_status_mask = ~chunk['status'].isin(self.valid_order_statuses)
        if invalid_status_mask.any():
            errors['invalid_statuses'] = chunk[invalid_status_mask]['order_id'].tolist()
        
        # Validate dates
        try:
            chunk['created_at'] = pd.to_datetime(chunk['created_at'], errors='coerce')
            
            # Check for invalid dates
            invalid_date_mask = chunk['created_at'].isnull()
            if invalid_date_mask.any():
                errors['invalid_dates'] = chunk[invalid_date_mask]['order_id'].tolist()
            
            # Check date logic (created_at should be in the past)
            future_date_mask = chunk['created_at'] > datetime.now()
            if future_date_mask.any():
                errors['date_logic_errors'] = chunk[future_date_mask]['order_id'].tolist()
                
        except Exception as e:
            errors['invalid_dates'] = [f"Date conversion error: {str(e)}"]
        
        # Validate num_of_item
        try:
            chunk['num_of_item'] = pd.to_numeric(chunk['num_of_item'], errors='coerce')
            invalid_num_mask = (chunk['num_of_item'] <= 0) | chunk['num_of_item'].isnull()
            if invalid_num_mask.any():
                errors['invalid_num_items'] = chunk[invalid_num_mask]['order_id'].tolist()
        except Exception as e:
            errors['invalid_num_items'] = [f"Numeric conversion error: {str(e)}"]
        
        total_errors = sum(len(error_list) for error_list in errors.values())
        
        return {
            'chunk_num': chunk_num,
            'total_errors': total_errors,
            'error_details': {k: v for k, v in errors.items() if v}
        }
    
    def validate_order_items_chunk(self, chunk: pd.DataFrame, chunk_num: int, reference_data: Dict) -> Dict:
        """Validate order items with referential integrity"""
        errors = {
            'null_ids': [],
            'orphaned_order_ids': [],
            'orphaned_product_ids': [],
            'invalid_statuses': [],
            'invalid_sale_prices': [],
            'user_id_mismatch': [],
            'invalid_dates': []
        }
        
        # Check for null IDs
        null_id_mask = chunk['id'].isnull()
        if null_id_mask.any():
            errors['null_ids'] = chunk[null_id_mask].index.tolist()
        
        # Referential integrity: order_id exists in orders
        chunk['order_id'] = chunk['order_id'].astype(str)
        orphaned_order_mask = ~chunk['order_id'].isin(reference_data['valid_order_ids'])
        if orphaned_order_mask.any():
            errors['orphaned_order_ids'] = chunk[orphaned_order_mask]['order_id'].tolist()
        
        # Referential integrity: product_id exists in products
        chunk['product_id'] = chunk['product_id'].astype(str)
        orphaned_product_mask = ~chunk['product_id'].isin(reference_data['valid_product_ids'])
        if orphaned_product_mask.any():
            errors['orphaned_product_ids'] = chunk[orphaned_product_mask]['product_id'].tolist()
        
        # Validate item statuses
        invalid_status_mask = ~chunk['status'].isin(self.valid_item_statuses)
        if invalid_status_mask.any():
            errors['invalid_statuses'] = chunk[invalid_status_mask]['id'].tolist()
        
        # Validate sale prices
        try:
            chunk['sale_price'] = pd.to_numeric(chunk['sale_price'], errors='coerce')
            invalid_price_mask = (chunk['sale_price'] <= 0) | chunk['sale_price'].isnull()
            if invalid_price_mask.any():
                errors['invalid_sale_prices'] = chunk[invalid_price_mask]['id'].tolist()
        except Exception as e:
            errors['invalid_sale_prices'] = [f"Price conversion error: {str(e)}"]
        
        # Validate dates
        try:
            chunk['created_at'] = pd.to_datetime(chunk['created_at'], errors='coerce')
            invalid_date_mask = chunk['created_at'].isnull()
            if invalid_date_mask.any():
                errors['invalid_dates'] = chunk[invalid_date_mask]['id'].tolist()
        except Exception as e:
            errors['invalid_dates'] = [f"Date conversion error: {str(e)}"]
        
        total_errors = sum(len(error_list) for error_list in errors.values())
        
        return {
            'chunk_num': chunk_num,
            'total_errors': total_errors,
            'error_details': {k: v for k, v in errors.items() if v}
        }

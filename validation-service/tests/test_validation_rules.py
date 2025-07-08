import pytest
import pandas as pd
from validation_rules import ValidationRules

class TestValidationRules:
    
    def setup_method(self):
        """Set up test environment"""
        self.validation_rules = ValidationRules()
    
    def test_validate_headers_success(self):
        """Test successful header validation"""
        # Test products headers
        products_headers = ['id', 'sku', 'cost', 'category', 'name', 'brand', 'retail_price', 'department']
        assert self.validation_rules.validate_headers('products', products_headers) == True
        
        # Test orders headers
        orders_headers = ['order_id', 'user_id', 'status', 'created_at', 'num_of_item']
        assert self.validation_rules.validate_headers('orders', orders_headers) == True
        
        # Test order_items headers
        order_items_headers = ['id', 'order_id', 'user_id', 'product_id', 'status', 'created_at', 'sale_price']
        assert self.validation_rules.validate_headers('order_items', order_items_headers) == True
    
    def test_validate_headers_failure(self):
        """Test header validation failures"""
        # Missing required headers
        incomplete_headers = ['id', 'sku', 'cost']  # Missing required fields
        assert self.validation_rules.validate_headers('products', incomplete_headers) == False
        
        # Wrong headers entirely
        wrong_headers = ['wrong', 'headers', 'here']
        assert self.validation_rules.validate_headers('products', wrong_headers) == False
    
    def test_validate_products_chunk_success(self):
        """Test successful products validation"""
        # Create valid products data
        products_data = pd.DataFrame({
            'id': [1, 2, 3],
            'sku': ['ABC-12345678', 'DEF-87654321', 'GHI-11111111'],
            'cost': [10.50, 25.00, 15.75],
            'category': ['Electronics', 'Clothing', 'Books'],
            'name': ['Product 1', 'Product 2', 'Product 3'],
            'brand': ['Brand A', 'Brand B', 'Brand C'],
            'retail_price': [20.00, 50.00, 30.00],
            'department': ['Tech', 'Fashion', 'Media']
        })
        
        result = self.validation_rules.validate_products_chunk(products_data, 0)
        
        assert result['total_errors'] == 0
        assert result['chunk_num'] == 0
        assert len(result['error_details']) == 0
    
    def test_validate_products_chunk_failures(self):
        """Test products validation with various errors"""
        # Create invalid products data
        products_data = pd.DataFrame({
            'id': [1, None, 3],  # Null ID
            'sku': ['ABC-12345678', 'INVALID_SKU', 'GHI-11111111'],  # Invalid SKU format
            'cost': [10.50, -5.00, 15.75],  # Negative cost
            'category': ['Electronics', 'Clothing', 'Books'],
            'name': ['Product 1', '', 'Product 3'],  # Empty name
            'brand': ['Brand A', 'Brand B', 'Brand C'],
            'retail_price': [20.00, 25.00, 30.00],  # Cost > retail_price for second item
            'department': ['Tech', 'Fashion', 'Media']
        })
        
        result = self.validation_rules.validate_products_chunk(products_data, 0)
        
        assert result['total_errors'] > 0
        assert 'null_ids' in result['error_details']
        assert 'invalid_skus' in result['error_details']
        assert 'negative_costs' in result['error_details']
        assert 'empty_names' in result['error_details']
        assert 'cost_greater_than_retail' in result['error_details']
    
    def test_validate_orders_chunk_success(self):
        """Test successful orders validation"""
        orders_data = pd.DataFrame({
            'order_id': [1, 2, 3],
            'user_id': [100, 101, 102],
            'status': ['delivered', 'shipped', 'pending'],
            'created_at': ['2025-01-01T10:00:00', '2025-01-02T11:00:00', '2025-01-03T12:00:00'],
            'num_of_item': [1, 2, 3]
        })
        
        # Mock reference data
        reference_data = {
            'valid_user_ids': {100, 101, 102}
        }
        
        result = self.validation_rules.validate_orders_chunk(orders_data, 0, reference_data)
        
        assert result['total_errors'] == 0
    
    def test_validate_orders_chunk_failures(self):
        """Test orders validation with errors"""
        orders_data = pd.DataFrame({
            'order_id': [1, None, 3],  # Null order_id
            'user_id': [100, 101, None],  # Null user_id
            'status': ['delivered', 'invalid_status', 'pending'],  # Invalid status
            'created_at': ['2025-01-01T10:00:00', 'invalid_date', '2025-01-03T12:00:00'],  # Invalid date
            'num_of_item': [1, -1, 0]  # Invalid quantities
        })
        
        reference_data = {
            'valid_user_ids': {100, 101, 102}
        }
        
        result = self.validation_rules.validate_orders_chunk(orders_data, 0, reference_data)
        
        assert result['total_errors'] > 0
        assert 'null_order_ids' in result['error_details']
        assert 'null_user_ids' in result['error_details']
        assert 'invalid_statuses' in result['error_details']
        assert 'invalid_dates' in result['error_details']
        assert 'invalid_num_items' in result['error_details']
    
    def test_validate_order_items_referential_integrity(self):
        """Test order items referential integrity validation"""
        order_items_data = pd.DataFrame({
            'id': [1, 2, 3],
            'order_id': [1, 2, 999],  # 999 doesn't exist in orders
            'user_id': [100, 101, 102],
            'product_id': [1, 2, 888],  # 888 doesn't exist in products
            'status': ['delivered', 'shipped', 'pending'],
            'created_at': ['2025-01-01T10:00:00', '2025-01-02T11:00:00', '2025-01-03T12:00:00'],
            'sale_price': [10.50, 25.00, 15.75]
        })
        
        reference_data = {
            'valid_order_ids': {'1', '2'},  # Missing '999'
            'valid_product_ids': {'1', '2'}  # Missing '888'
        }
        
        result = self.validation_rules.validate_order_items_chunk(order_items_data, 0, reference_data)
        
        assert result['total_errors'] > 0
        assert 'orphaned_order_ids' in result['error_details']
        assert 'orphaned_product_ids' in result['error_details']
        assert '999' in result['error_details']['orphaned_order_ids']
        assert '888' in result['error_details']['orphaned_product_ids']

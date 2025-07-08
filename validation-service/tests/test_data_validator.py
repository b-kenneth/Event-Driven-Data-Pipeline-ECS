import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from app import DataValidator

class TestDataValidator:
    
    def setup_method(self):
        """Set up test environment"""
        with patch.dict('os.environ', {
            'S3_BUCKET_NAME': 'test-bucket',
            'BATCH_ID': 'test-batch-001',
            'PRODUCTS_FILES': '["raw-data/products/product1.csv"]',
            'ORDERS_FILES': '["raw-data/orders/order1.csv"]',
            'ORDER_ITEMS_FILES': '["raw-data/order-items/item1.csv"]'
        }):
            self.validator = DataValidator()
    
    @patch('app.S3Handler')
    @patch('app.ValidationRules')
    def test_validate_batch_success(self, mock_validation_rules, mock_s3_handler):
        """Test successful batch validation"""
        # Mock S3Handler
        mock_s3_instance = Mock()
        mock_s3_handler.return_value = mock_s3_instance
        
        # Mock successful file reading
        mock_s3_instance.read_csv_sample.return_value = pd.DataFrame({
            'id': [1], 'name': ['Product 1']
        })
        
        mock_s3_instance.read_csv.return_value = pd.DataFrame({
            'id': [1, 2], 'name': ['Product 1', 'Product 2']
        })
        
        mock_s3_instance.read_csv_chunks.return_value = [
            pd.DataFrame({'id': [1], 'name': ['Product 1']})
        ]
        
        # Mock ValidationRules
        mock_rules_instance = Mock()
        mock_validation_rules.return_value = mock_rules_instance
        mock_rules_instance.validate_headers.return_value = True
        mock_rules_instance.validate_products_chunk.return_value = {
            'total_errors': 0,
            'error_details': {}
        }
        mock_rules_instance.validate_orders_chunk.return_value = {
            'total_errors': 0,
            'error_details': {}
        }
        mock_rules_instance.validate_order_items_chunk.return_value = {
            'total_errors': 0,
            'error_details': {}
        }
        
        # Execute validation
        result = self.validator.validate_batch()
        
        # Assertions
        assert result == True
        mock_rules_instance.validate_headers.assert_called()
        mock_s3_instance.read_csv_sample.assert_called()
    
    @patch('app.S3Handler')
    @patch('app.ValidationRules')
    def test_validate_batch_structure_failure(self, mock_validation_rules, mock_s3_handler):
        """Test batch validation with structure failure"""
        # Mock S3Handler
        mock_s3_instance = Mock()
        mock_s3_handler.return_value = mock_s3_instance
        
        mock_s3_instance.read_csv_sample.return_value = pd.DataFrame({
            'wrong': [1], 'headers': ['Product 1']
        })
        
        # Mock ValidationRules to return False for header validation
        mock_rules_instance = Mock()
        mock_validation_rules.return_value = mock_rules_instance
        mock_rules_instance.validate_headers.return_value = False
        
        # Execute validation
        result = self.validator.validate_batch()
        
        # Assertions
        assert result == False
    
    @patch('app.S3Handler')
    @patch('app.ValidationRules')
    def test_validate_batch_data_errors(self, mock_validation_rules, mock_s3_handler):
        """Test batch validation with data quality errors"""
        # Mock S3Handler
        mock_s3_instance = Mock()
        mock_s3_handler.return_value = mock_s3_instance
        
        mock_s3_instance.read_csv_sample.return_value = pd.DataFrame({
            'id': [1], 'name': ['Product 1']
        })
        
        mock_s3_instance.read_csv.return_value = pd.DataFrame({
            'id': [1, 2], 'name': ['Product 1', 'Product 2']
        })
        
        mock_s3_instance.read_csv_chunks.return_value = [
            pd.DataFrame({'id': [1], 'name': ['Product 1']})
        ]
        
        # Mock ValidationRules with errors
        mock_rules_instance = Mock()
        mock_validation_rules.return_value = mock_rules_instance
        mock_rules_instance.validate_headers.return_value = True
        mock_rules_instance.validate_products_chunk.return_value = {
            'total_errors': 2,
            'error_details': {
                'null_ids': [1],
                'invalid_skus': [2]
            }
        }
        mock_rules_instance.validate_orders_chunk.return_value = {
            'total_errors': 0,
            'error_details': {}
        }
        mock_rules_instance.validate_order_items_chunk.return_value = {
            'total_errors': 0,
            'error_details': {}
        }
        
        # Execute validation
        result = self.validator.validate_batch()
        
        # Assertions
        assert result == False  # Should fail due to data errors

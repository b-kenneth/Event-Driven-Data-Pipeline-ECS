import json
import boto3
import pandas as pd
import os
import sys
import logging
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional
import traceback

from validation_rules import ValidationRules
from utils import setup_logging, S3Handler, ValidationReport

class DataValidator:
    def __init__(self):
        self.s3_handler = S3Handler()
        self.validation_rules = ValidationRules()
        self.logger = setup_logging()
        self.bucket_name = os.environ.get('S3_BUCKET_NAME')
        self.batch_id = os.environ.get('BATCH_ID', f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        
        # Parse file lists from environment variables (NEW: handles multiple files)
        self.products_files = json.loads(os.environ.get('PRODUCTS_FILES', '[]'))
        self.orders_files = json.loads(os.environ.get('ORDERS_FILES', '[]'))
        self.order_items_files = json.loads(os.environ.get('ORDER_ITEMS_FILES', '[]'))
        
        # Backward compatibility: if old single file env vars exist, use them
        if not self.products_files and os.environ.get('PRODUCTS_FILE_KEY'):
            self.products_files = [os.environ.get('PRODUCTS_FILE_KEY')]
        if not self.orders_files and os.environ.get('ORDERS_FILE_KEY'):
            self.orders_files = [os.environ.get('ORDERS_FILE_KEY')]
        if not self.order_items_files and os.environ.get('ORDER_ITEMS_FILE_KEY'):
            self.order_items_files = [os.environ.get('ORDER_ITEMS_FILE_KEY')]
        
        self.validation_report = ValidationReport(self.batch_id)
        
    def validate_batch(self) -> bool:
        """Main validation orchestrator for the complete batch"""
        try:
            self.logger.info(f"🚀 Starting validation for batch: {self.batch_id}")
            self.logger.info(f"📁 Files to process: {len(self.products_files)} products, {len(self.orders_files)} orders, {len(self.order_items_files)} order_items")
            
            # Stage 1: Structure validation (fail fast)
            if not self._validate_file_structures():
                self.logger.error("❌ Structure validation failed - aborting batch")
                return False
            
            # Stage 2: Load reference data
            reference_data = self._load_reference_data()
            if not reference_data:
                self.logger.error("❌ Failed to load reference data - aborting batch")
                return False
            
            # Stage 3: Validate each dataset
            validation_results = {}
            
            # Validate products (reference data)
            validation_results['products'] = self._validate_products()
            
            # Validate orders
            validation_results['orders'] = self._validate_orders(reference_data)
            
            # Validate order items (most complex - referential integrity)
            validation_results['order_items'] = self._validate_order_items(reference_data)
            
            # Stage 4: Generate final report
            overall_success = all(result['success'] for result in validation_results.values())
            
            self._generate_validation_report(validation_results, overall_success)
            
            if overall_success:
                self.logger.info("✅ Batch validation completed successfully")
                return True
            else:
                self.logger.error("❌ Batch validation failed")
                return False
                
        except Exception as e:
            self.logger.error(f"💥 Critical validation error: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False
    
    def _validate_file_structures(self) -> bool:
        """Stage 1: Fast structure validation for ALL files"""
        self.logger.info("🔍 Stage 1: Validating file structures for all files")
        
        file_groups = {
            'products': self.products_files,
            'orders': self.orders_files,
            'order_items': self.order_items_files
        }
        
        for file_type, file_list in file_groups.items():
            if not file_list:
                self.logger.error(f"No {file_type} files found")
                return False
                
            self.logger.info(f"Validating {len(file_list)} {file_type} files")
            
            for file_key in file_list:
                try:
                    # Check if file exists and validate headers
                    df_sample = self.s3_handler.read_csv_sample(self.bucket_name, file_key, nrows=5)
                    
                    if not self.validation_rules.validate_headers(file_type, df_sample.columns.tolist()):
                        self.logger.error(f"Invalid headers for {file_type} file: {file_key}")
                        return False
                        
                    self.logger.info(f"✅ Structure validation passed for {file_type}: {file_key}")
                    
                except Exception as e:
                    self.logger.error(f"Failed to read {file_type} file {file_key}: {str(e)}")
                    return False
        
        return True
    
    def _load_reference_data(self) -> Optional[Dict]:
        """Stage 2: Load reference data into memory from ALL files"""
        self.logger.info("📚 Stage 2: Loading reference data from all files")
        
        try:
            # Load and combine all products files
            all_products_dfs = []
            for products_file in self.products_files:
                df = self.s3_handler.read_csv(self.bucket_name, products_file)
                all_products_dfs.append(df)
                self.logger.info(f"Loaded products file: {products_file} ({len(df)} records)")
            
            products_df = pd.concat(all_products_dfs, ignore_index=True)
            
            # Load and combine all orders files
            all_orders_dfs = []
            for orders_file in self.orders_files:
                df = self.s3_handler.read_csv(self.bucket_name, orders_file)
                all_orders_dfs.append(df)
                self.logger.info(f"Loaded orders file: {orders_file} ({len(df)} records)")
            
            orders_df = pd.concat(all_orders_dfs, ignore_index=True)
            
            reference_data = {
                'products_df': products_df,
                'orders_df': orders_df,
                'valid_product_ids': set(products_df['id'].astype(str)),
                'valid_order_ids': set(orders_df['order_id'].astype(str)),
                'valid_user_ids': set(orders_df['user_id'].astype(str)),
                'valid_categories': set(products_df['category']),
                'valid_departments': set(products_df['department']),
                'product_price_map': products_df.set_index('id')[['cost', 'retail_price']].to_dict('index')
            }
            
            self.logger.info(f"✅ Reference data loaded: {len(reference_data['valid_product_ids'])} products, {len(reference_data['valid_order_ids'])} orders")
            return reference_data
            
        except Exception as e:
            self.logger.error(f"Failed to load reference data: {str(e)}")
            return None
    
    def _validate_products(self) -> Dict:
        """Validate ALL products files"""
        self.logger.info(f"🛍️ Stage 3a: Validating {len(self.products_files)} products files")
        
        try:
            all_chunk_results = []
            total_records = 0
            
            # Process each products file
            for file_idx, file_key in enumerate(self.products_files):
                self.logger.info(f"Processing products file {file_idx + 1}/{len(self.products_files)}: {file_key}")
                
                # Process in chunks for scalability
                for chunk_num, chunk in enumerate(self.s3_handler.read_csv_chunks(
                    self.bucket_name, file_key, chunksize=10000
                )):
                    total_records += len(chunk)
                    
                    chunk_result = self.validation_rules.validate_products_chunk(chunk, f"{file_idx}_{chunk_num}")
                    all_chunk_results.append(chunk_result)
                    
                    self.logger.info(f"Processed products chunk {file_idx}_{chunk_num}: {len(chunk)} records")
            
            # Aggregate results from all files
            total_errors = sum(result['total_errors'] for result in all_chunk_results)
            error_details = {}
            for result in all_chunk_results:
                for error_type, errors in result['error_details'].items():
                    if error_type not in error_details:
                        error_details[error_type] = []
                    error_details[error_type].extend(errors)
            
            success = total_errors == 0
            
            result = {
                'success': success,
                'total_records': total_records,
                'valid_records': total_records - total_errors,
                'total_errors': total_errors,
                'error_details': error_details,
                'files_processed': len(self.products_files)
            }
            
            self.logger.info(f"✅ Products validation completed: {len(self.products_files)} files, {total_records} total records")
            self.validation_report.add_dataset_result('products', result)
            return result
            
        except Exception as e:
            self.logger.error(f"Products validation failed: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def _validate_orders(self, reference_data: Dict) -> Dict:
        """Validate ALL orders files"""
        self.logger.info(f"📦 Stage 3b: Validating {len(self.orders_files)} orders files")
        
        try:
            all_chunk_results = []
            total_records = 0
            
            # Process each orders file
            for file_idx, file_key in enumerate(self.orders_files):
                self.logger.info(f"Processing orders file {file_idx + 1}/{len(self.orders_files)}: {file_key}")
                
                for chunk_num, chunk in enumerate(self.s3_handler.read_csv_chunks(
                    self.bucket_name, file_key, chunksize=10000
                )):
                    total_records += len(chunk)
                    
                    chunk_result = self.validation_rules.validate_orders_chunk(
                        chunk, f"{file_idx}_{chunk_num}", reference_data
                    )
                    all_chunk_results.append(chunk_result)
                    
                    self.logger.info(f"Processed orders chunk {file_idx}_{chunk_num}: {len(chunk)} records")
            
            # Aggregate results
            total_errors = sum(result['total_errors'] for result in all_chunk_results)
            error_details = {}
            for result in all_chunk_results:
                for error_type, errors in result['error_details'].items():
                    if error_type not in error_details:
                        error_details[error_type] = []
                    error_details[error_type].extend(errors)
            
            success = total_errors == 0
            
            result = {
                'success': success,
                'total_records': total_records,
                'valid_records': total_records - total_errors,
                'total_errors': total_errors,
                'error_details': error_details,
                'files_processed': len(self.orders_files)
            }
            
            self.logger.info(f"✅ Orders validation completed: {len(self.orders_files)} files, {total_records} total records")
            self.validation_report.add_dataset_result('orders', result)
            return result
            
        except Exception as e:
            self.logger.error(f"Orders validation failed: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def _validate_order_items(self, reference_data: Dict) -> Dict:
        """Validate ALL order items files with referential integrity"""
        self.logger.info(f"🛒 Stage 3c: Validating {len(self.order_items_files)} order items files")
        
        try:
            all_chunk_results = []
            total_records = 0
            
            # Process each order items file
            for file_idx, file_key in enumerate(self.order_items_files):
                self.logger.info(f"Processing order items file {file_idx + 1}/{len(self.order_items_files)}: {file_key}")
                
                for chunk_num, chunk in enumerate(self.s3_handler.read_csv_chunks(
                    self.bucket_name, file_key, chunksize=10000
                )):
                    total_records += len(chunk)
                    
                    chunk_result = self.validation_rules.validate_order_items_chunk(
                        chunk, f"{file_idx}_{chunk_num}", reference_data
                    )
                    all_chunk_results.append(chunk_result)
                    
                    self.logger.info(f"Processed order items chunk {file_idx}_{chunk_num}: {len(chunk)} records")
            
            # Aggregate results
            total_errors = sum(result['total_errors'] for result in all_chunk_results)
            error_details = {}
            for result in all_chunk_results:
                for error_type, errors in result['error_details'].items():
                    if error_type not in error_details:
                        error_details[error_type] = []
                    error_details[error_type].extend(errors)
            
            success = total_errors == 0
            
            result = {
                'success': success,
                'total_records': total_records,
                'valid_records': total_records - total_errors,
                'total_errors': total_errors,
                'error_details': error_details,
                'files_processed': len(self.order_items_files)
            }
            
            self.logger.info(f"✅ Order items validation completed: {len(self.order_items_files)} files, {total_records} total records")
            self.validation_report.add_dataset_result('order_items', result)
            return result
            
        except Exception as e:
            self.logger.error(f"Order items validation failed: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def _generate_validation_report(self, validation_results: Dict, overall_success: bool):
        """Generate and store comprehensive validation report"""
        
        self.validation_report.set_overall_status(overall_success)
        self.validation_report.add_summary(validation_results)
        
        # Store report in S3
        report_key = f"validation-reports/{self.batch_id}/validation_report.json"
        self.s3_handler.upload_json(
            self.bucket_name, 
            report_key, 
            self.validation_report.to_dict()
        )
        
        self.logger.info(f"📊 Validation report stored: s3://{self.bucket_name}/{report_key}")

def main():
    """Main entry point"""
    validator = DataValidator()
    
    # Validate the batch
    success = validator.validate_batch()
    
    if success:
        print("✅ VALIDATION_SUCCESS")
        sys.exit(0)
    else:
        print("❌ VALIDATION_FAILED")
        sys.exit(1)

if __name__ == "__main__":
    main()

import json
import boto3
import os
import sys
import logging
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional
import traceback

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from kpi_calculator import KPICalculator
from dynamodb_handler import DynamoDBHandler
from utils import setup_logging, S3Handler, TransformationReport

class DataTransformer:
    def __init__(self):
        self.logger = setup_logging()
        self.s3_handler = S3Handler()
        self.dynamodb_handler = DynamoDBHandler()
        
        # Initialize Spark session with S3 dependencies
        self.spark = SparkSession.builder \
            .appName("ECommerceDataTransformation") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
            .getOrCreate()
    
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Environment variables
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
        
        self.transformation_report = TransformationReport(self.batch_id)
        self.kpi_calculator = KPICalculator(self.spark)
        
    def transform_batch(self) -> bool:
        """Main transformation orchestrator"""
        try:
            self.logger.info(f"🚀 Starting transformation for batch: {self.batch_id}")
            self.logger.info(f"📁 Files to process: {len(self.products_files)} products, {len(self.orders_files)} orders, {len(self.order_items_files)} order_items")
            
            # Stage 1: Load validated data
            dataframes = self._load_validated_data()
            if not dataframes:
                self.logger.error("❌ Failed to load validated data")
                return False
            
            # Stage 2: Compute KPIs
            kpis = self._compute_kpis(dataframes)
            if not kpis:
                self.logger.error("❌ Failed to compute KPIs")
                return False
            
            # Stage 3: Store KPIs in DynamoDB
            storage_success = self._store_kpis(kpis)
            if not storage_success:
                self.logger.error("❌ Failed to store KPIs in DynamoDB")
                return False
            
            # Stage 4: Generate transformation report
            self._generate_transformation_report(kpis)
            
            self.logger.info("✅ Batch transformation completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"💥 Critical transformation error: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False
        finally:
            self.spark.stop()
    
    def _load_validated_data(self) -> Optional[Dict]:
        """Stage 1: Load ALL validated data from S3"""
        self.logger.info("📚 Stage 1: Loading all validated data")
        
        try:
            # Load and union all files of each type
            products_df = self._load_multiple_files(self.products_files, "products")
            orders_df = self._load_multiple_files(self.orders_files, "orders")  
            order_items_df = self._load_multiple_files(self.order_items_files, "order_items")
            
            # Cache dataframes for multiple operations
            products_df.cache()
            orders_df.cache()
            order_items_df.cache()
            
            # Log data counts
            products_count = products_df.count()
            orders_count = orders_df.count()
            order_items_count = order_items_df.count()
            
            self.logger.info(f"✅ All data loaded: {products_count} products, {orders_count} orders, {order_items_count} order items")
            self.logger.info(f"📁 Files processed: {len(self.products_files)} products, {len(self.orders_files)} orders, {len(self.order_items_files)} order_items")
            
            return {
                'products': products_df,
                'orders': orders_df,
                'order_items': order_items_df,
                'counts': {
                    'products': products_count,
                    'orders': orders_count,
                    'order_items': order_items_count
                },
                'files_processed': {
                    'products': len(self.products_files),
                    'orders': len(self.orders_files),
                    'order_items': len(self.order_items_files)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to load data: {str(e)}")
            return None

    def _load_multiple_files(self, file_list: List[str], file_type: str):
        """Load and union multiple files of the same type"""
        
        if not file_list:
            raise ValueError(f"No {file_type} files provided")
        
        self.logger.info(f"Loading {len(file_list)} {file_type} files")
        
        # Define schemas for better performance
        schemas = {
            'products': StructType([
                StructField("id", IntegerType(), True),
                StructField("sku", StringType(), True),
                StructField("cost", DoubleType(), True),
                StructField("category", StringType(), True),
                StructField("name", StringType(), True),
                StructField("brand", StringType(), True),
                StructField("retail_price", DoubleType(), True),
                StructField("department", StringType(), True)
            ]),
            'orders': StructType([
                StructField("order_id", IntegerType(), True),
                StructField("user_id", IntegerType(), True),
                StructField("status", StringType(), True),
                StructField("created_at", TimestampType(), True),
                StructField("returned_at", TimestampType(), True),
                StructField("shipped_at", TimestampType(), True),
                StructField("delivered_at", TimestampType(), True),
                StructField("num_of_item", IntegerType(), True)
            ]),
            'order_items': StructType([
                StructField("id", IntegerType(), True),
                StructField("order_id", IntegerType(), True),
                StructField("user_id", IntegerType(), True),
                StructField("product_id", IntegerType(), True),
                StructField("status", StringType(), True),
                StructField("created_at", TimestampType(), True),
                StructField("shipped_at", TimestampType(), True),
                StructField("delivered_at", TimestampType(), True),
                StructField("returned_at", TimestampType(), True),
                StructField("sale_price", DoubleType(), True)
            ])
        }
        
        # Load first file to get schema
        first_df = self.spark.read \
            .option("header", "true") \
            .schema(schemas[file_type]) \
            .csv(f"s3a://{self.bucket_name}/{file_list[0]}")
        
        self.logger.info(f"✅ Loaded {file_type} file: {file_list[0]}")
        
        # If only one file, return it
        if len(file_list) == 1:
            return first_df
        
        # Union all files
        combined_df = first_df
        for file_key in file_list[1:]:
            df = self.spark.read \
                .option("header", "true") \
                .schema(schemas[file_type]) \
                .csv(f"s3a://{self.bucket_name}/{file_key}")
            
            combined_df = combined_df.union(df)
            self.logger.info(f"✅ Added {file_type} file: {file_key}")
        
        self.logger.info(f"✅ Combined {len(file_list)} {file_type} files into single DataFrame")
        return combined_df
    
    def _compute_kpis(self, dataframes: Dict) -> Optional[Dict]:
        """Stage 2: Compute business KPIs"""
        self.logger.info("🧮 Stage 2: Computing business KPIs")
        
        try:
            # Extract dataframes
            products_df = dataframes['products']
            orders_df = dataframes['orders']
            order_items_df = dataframes['order_items']
            
            # Compute Category-Level KPIs
            self.logger.info("Computing Category-Level KPIs...")
            category_kpis = self.kpi_calculator.compute_category_kpis(
                products_df, orders_df, order_items_df
            )
            
            # Compute Order-Level KPIs
            self.logger.info("Computing Order-Level KPIs...")
            order_kpis = self.kpi_calculator.compute_order_kpis(
                orders_df, order_items_df
            )
            
            # Convert to Pandas for DynamoDB storage
            category_kpis_pd = category_kpis.toPandas()
            order_kpis_pd = order_kpis.toPandas()
            
            self.logger.info(f"✅ KPIs computed: {len(category_kpis_pd)} category KPIs, {len(order_kpis_pd)} order KPIs")
            
            return {
                'category_kpis': category_kpis_pd,
                'order_kpis': order_kpis_pd,
                'spark_dfs': {
                    'category_kpis': category_kpis,
                    'order_kpis': order_kpis
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to compute KPIs: {str(e)}")
            return None
    
    def _store_kpis(self, kpis: Dict) -> bool:
        """Stage 3: Store KPIs in DynamoDB"""
        self.logger.info("💾 Stage 3: Storing KPIs in DynamoDB")
        
        try:
            # Store Category-Level KPIs
            category_success = self.dynamodb_handler.store_category_kpis(
                kpis['category_kpis']
            )
            
            # Store Order-Level KPIs
            order_success = self.dynamodb_handler.store_order_kpis(
                kpis['order_kpis']
            )
            
            if category_success and order_success:
                self.logger.info("✅ All KPIs stored successfully in DynamoDB")
                return True
            else:
                self.logger.error("❌ Failed to store some KPIs in DynamoDB")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to store KPIs: {str(e)}")
            return False
    
    def _generate_transformation_report(self, kpis: Dict):
        """Generate and store transformation report"""
        
        self.transformation_report.set_kpi_counts(
            len(kpis['category_kpis']),
            len(kpis['order_kpis'])
        )
        
        self.transformation_report.set_status(True)
        
        # Store report in S3
        report_key = f"transformation-reports/{self.batch_id}/transformation_report.json"
        self.s3_handler.upload_json(
            self.bucket_name,
            report_key,
            self.transformation_report.to_dict()
        )
        
        self.logger.info(f"📊 Transformation report stored: s3://{self.bucket_name}/{report_key}")

def main():
    """Main entry point"""
    transformer = DataTransformer()
    
    # Transform the batch
    success = transformer.transform_batch()
    
    if success:
        print("✅ TRANSFORMATION_SUCCESS")
        sys.exit(0)
    else:
        print("❌ TRANSFORMATION_FAILED")
        sys.exit(1)

if __name__ == "__main__":
    main()

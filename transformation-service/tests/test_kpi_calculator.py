import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from kpi_calculator import KPICalculator

class TestKPICalculator:
    
    @classmethod
    def setup_class(cls):
        """Set up Spark session for testing"""
        cls.spark = SparkSession.builder \
            .appName("TestKPICalculator") \
            .master("local[2]") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .getOrCreate()
        
        cls.kpi_calculator = KPICalculator(cls.spark)
    
    @classmethod
    def teardown_class(cls):
        """Clean up Spark session"""
        cls.spark.stop()
    
    def test_compute_category_kpis(self):
        """Test category-level KPI computation"""
        # Create test data
        products_data = [
            (1, "Electronics", "Product 1"),
            (2, "Clothing", "Product 2"),
            (3, "Electronics", "Product 3")
        ]
        products_df = self.spark.createDataFrame(
            products_data,
            ["id", "category", "name"]
        )
        
        orders_data = [
            (1, 100, "delivered", "2025-01-01T10:00:00"),
            (2, 101, "returned", "2025-01-01T11:00:00"),
            (3, 102, "delivered", "2025-01-02T10:00:00")
        ]
        orders_df = self.spark.createDataFrame(
            orders_data,
            ["order_id", "user_id", "status", "created_at"]
        ).withColumn("created_at", col("created_at").cast(TimestampType()))
        
        order_items_data = [
            (1, 1, 100, 1, "delivered", "2025-01-01T10:00:00", 50.0),
            (2, 2, 101, 2, "returned", "2025-01-01T11:00:00", 75.0),
            (3, 3, 102, 3, "delivered", "2025-01-02T10:00:00", 100.0)
        ]
        order_items_df = self.spark.createDataFrame(
            order_items_data,
            ["id", "order_id", "user_id", "product_id", "status", "created_at", "sale_price"]
        ).withColumn("created_at", col("created_at").cast(TimestampType()))
        
        # Compute KPIs
        result_df = self.kpi_calculator.compute_category_kpis(
            products_df, orders_df, order_items_df
        )
        
        # Collect results
        results = result_df.collect()
        
        # Assertions
        assert len(results) >= 2  # Should have at least 2 category-date combinations
        
        # Check that we have the expected columns
        expected_columns = ['category', 'order_date', 'daily_revenue', 'avg_order_value', 'avg_return_rate']
        assert all(col in result_df.columns for col in expected_columns)
        
        # Verify data types and calculations
        electronics_row = [row for row in results if row['category'] == 'Electronics'][0]
        assert electronics_row['daily_revenue'] > 0
        assert electronics_row['avg_order_value'] > 0
        assert 0 <= electronics_row['avg_return_rate'] <= 1
    
    def test_compute_order_kpis(self):
        """Test order-level KPI computation"""
        # Create test data
        orders_data = [
            (1, 100, "delivered", "2025-01-01T10:00:00"),
            (2, 101, "returned", "2025-01-01T11:00:00"),
            (3, 102, "delivered", "2025-01-02T10:00:00"),
            (4, 100, "delivered", "2025-01-01T12:00:00")  # Same user, different order
        ]
        orders_df = self.spark.createDataFrame(
            orders_data,
            ["order_id", "user_id", "status", "created_at"]
        ).withColumn("created_at", col("created_at").cast(TimestampType()))
        
        order_items_data = [
            (1, 1, 100, 1, "delivered", "2025-01-01T10:00:00", 50.0),
            (2, 1, 100, 2, "delivered", "2025-01-01T10:00:00", 25.0),  # Same order, multiple items
            (3, 2, 101, 3, "returned", "2025-01-01T11:00:00", 75.0),
            (4, 3, 102, 1, "delivered", "2025-01-02T10:00:00", 100.0),
            (5, 4, 100, 2, "delivered", "2025-01-01T12:00:00", 30.0)
        ]
        order_items_df = self.spark.createDataFrame(
            order_items_data,
            ["id", "order_id", "user_id", "product_id", "status", "created_at", "sale_price"]
        ).withColumn("created_at", col("created_at").cast(TimestampType()))
        
        # Compute KPIs
        result_df = self.kpi_calculator.compute_order_kpis(orders_df, order_items_df)
        
        # Collect results
        results = result_df.collect()
        
        # Assertions
        assert len(results) == 2  # Should have 2 days of data
        
        # Check columns
        expected_columns = ['order_date', 'total_orders', 'total_revenue', 'total_items_sold', 'unique_customers', 'return_rate']
        assert all(col in result_df.columns for col in expected_columns)
        
        # Verify calculations for 2025-01-01
        jan_1_row = [row for row in results if str(row['order_date']) == '2025-01-01'][0]
        assert jan_1_row['total_orders'] == 3  # 3 orders on Jan 1
        assert jan_1_row['total_revenue'] == 180.0  # 75 + 75 + 30
        assert jan_1_row['total_items_sold'] == 4  # 4 items total
        assert jan_1_row['unique_customers'] == 2  # Users 100 and 101
        assert jan_1_row['return_rate'] == 1/3  # 1 returned out of 3 orders

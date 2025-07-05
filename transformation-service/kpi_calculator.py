from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

class KPICalculator:
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def compute_category_kpis(self, products_df, orders_df, order_items_df):
        """Compute Category-Level KPIs (Per Category, Per Day)"""
        
        # Join order items with products to get category information
        items_with_products = order_items_df.join(
            products_df.select("id", "category"),
            order_items_df.product_id == products_df.id,
            "inner"
        ).select(
            order_items_df["*"],
            products_df.category
        )
        
        # Join with orders to get order date and status
        enriched_items = items_with_products.join(
            orders_df.select("order_id", "created_at", "status"),
            "order_id",
            "inner"
        ).select(
            items_with_products["*"],
            orders_df.created_at.alias("order_date_full"),
            orders_df.status.alias("order_status")
        )
        
        # Extract date from timestamp
        enriched_items = enriched_items.withColumn(
            "order_date",
            to_date(col("order_date_full"))
        )
        
        # Add return flag
        enriched_items = enriched_items.withColumn(
            "is_returned",
            when(col("status") == "returned", 1).otherwise(0)
        )
        
        # Compute category-level aggregations
        category_kpis = enriched_items.groupBy("category", "order_date").agg(
            sum("sale_price").alias("daily_revenue"),
            avg("sale_price").alias("avg_order_value"),
            avg("is_returned").alias("avg_return_rate")
        ).orderBy("category", "order_date")
        
        # Round numeric values
        category_kpis = category_kpis.withColumn(
            "daily_revenue", round(col("daily_revenue"), 2)
        ).withColumn(
            "avg_order_value", round(col("avg_order_value"), 2)
        ).withColumn(
            "avg_return_rate", round(col("avg_return_rate"), 4)
        )
        
        return category_kpis
    
    def compute_order_kpis(self, orders_df, order_items_df):
        """Compute Order-Level KPIs (Per Day)"""
        
        # Extract date from order timestamp
        orders_with_date = orders_df.withColumn(
            "order_date",
            to_date(col("created_at"))
        )
        
        # Add return flag to orders
        orders_with_date = orders_with_date.withColumn(
            "is_returned",
            when(col("status") == "returned", 1).otherwise(0)
        )
        
        # Aggregate order items by order_id to get order totals
        order_totals = order_items_df.groupBy("order_id").agg(
            sum("sale_price").alias("order_total"),
            count("id").alias("items_in_order")
        )
        
        # Join orders with order totals
        orders_enriched = orders_with_date.join(
            order_totals,
            "order_id",
            "inner"
        )
        
        # Compute daily order-level KPIs
        order_kpis = orders_enriched.groupBy("order_date").agg(
            countDistinct("order_id").alias("total_orders"),
            sum("order_total").alias("total_revenue"),
            sum("items_in_order").alias("total_items_sold"),
            countDistinct("user_id").alias("unique_customers"),
            avg("is_returned").alias("return_rate")
        ).orderBy("order_date")
        
        # Round numeric values
        order_kpis = order_kpis.withColumn(
            "total_revenue", round(col("total_revenue"), 2)
        ).withColumn(
            "return_rate", round(col("return_rate"), 4)
        )
        
        return order_kpis

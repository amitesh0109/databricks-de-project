# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Business Analytics

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

print("Starting Gold Layer...")

# COMMAND ----------

# Setup
spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce_gold")
spark.sql("USE ecommerce_gold")

# Load silver tables
silver_customers = spark.table("ecommerce_silver.customers")
silver_products = spark.table("ecommerce_silver.products")
silver_orders = spark.table("ecommerce_silver.orders")
silver_order_items = spark.table("ecommerce_silver.order_items")
customer_summary = spark.table("ecommerce_silver.customer_summary")

# COMMAND ----------

# Sales dashboard metrics
sales_dashboard = (silver_orders
    .groupBy("order_year", "order_month", "status")
    .agg(
        F.count("order_id").alias("order_count"),
        F.sum("total_amount").alias("revenue"),
        F.avg("total_amount").alias("avg_order_value"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum("shipping_cost").alias("total_shipping"),
        F.sum("tax_amount").alias("total_tax")
    )
    .withColumn("month_year", F.concat(F.col("order_year"), F.lit("-"), 
                                     F.lpad(F.col("order_month"), 2, "0")))
    .withColumn("revenue_per_customer", F.round(F.col("revenue") / F.col("unique_customers"), 2))
)

sales_dashboard.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("order_year") \
    .saveAsTable("ecommerce_gold.sales_dashboard")

print(f"Sales dashboard: {sales_dashboard.count()} records")

# COMMAND ----------

# Customer segments analysis  
customer_segments = (customer_summary
    .withColumn("customer_tier",
        F.when(F.col("total_spent") >= 1000, "VIP")
        .when(F.col("total_spent") >= 500, "Premium") 
        .when(F.col("total_spent") >= 200, "Standard")
        .otherwise("Basic"))
    .withColumn("recency_status",
        F.when(F.col("days_since_last_order") <= 30, "Active")
        .when(F.col("days_since_last_order") <= 90, "Declining")
        .otherwise("Inactive"))
    .withColumn("frequency_tier",
        F.when(F.col("total_orders") >= 10, "High")
        .when(F.col("total_orders") >= 5, "Medium")
        .otherwise("Low"))
)

segment_summary = (customer_segments
    .groupBy("customer_tier", "recency_status", "segment", "country")
    .agg(
        F.count("customer_id").alias("customer_count"),
        F.sum("total_spent").alias("total_revenue"),
        F.avg("total_spent").alias("avg_customer_value"),
        F.avg("total_orders").alias("avg_orders_per_customer"),
        F.avg("avg_order_value").alias("avg_order_size")
    )
    .withColumn("revenue_per_customer", F.round(F.col("total_revenue") / F.col("customer_count"), 2))
)

segment_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_gold.customer_segments")

print(f"Customer segments: {segment_summary.count()} records")

# COMMAND ----------

# Product performance analysis
product_performance = (silver_order_items
    .groupBy("category", "price_category")
    .agg(
        F.sum("line_total").alias("category_revenue"),
        F.sum("profit").alias("category_profit"),
        F.sum("quantity").alias("units_sold"),
        F.countDistinct("order_id").alias("orders_with_category"),
        F.avg("unit_price").alias("avg_selling_price"),
        F.avg("discount_percent").alias("avg_discount_percent")
    )
    .withColumn("revenue_per_order", F.round(F.col("category_revenue") / F.col("orders_with_category"), 2))
    .withColumn("profit_margin_pct", F.round(F.col("category_profit") / F.col("category_revenue") * 100, 2))
    .withColumn("avg_units_per_order", F.round(F.col("units_sold") / F.col("orders_with_category"), 2))
)

product_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_gold.product_performance")

print(f"Product performance: {product_performance.count()} records")

# COMMAND ----------

# Executive KPIs
executive_kpis = spark.sql("""
SELECT 
    'Overall' as period,
    COUNT(DISTINCT order_id) as total_orders,
    ROUND(SUM(total_amount), 2) as total_revenue,
    COUNT(DISTINCT customer_id) as active_customers,
    ROUND(AVG(total_amount), 2) as avg_order_value,
    ROUND(SUM(total_amount) / COUNT(DISTINCT customer_id), 2) as revenue_per_customer,
    COUNT(DISTINCT CASE WHEN status = 'Delivered' THEN order_id END) as completed_orders,
    ROUND(COUNT(DISTINCT CASE WHEN status = 'Delivered' THEN order_id END) * 100.0 / COUNT(DISTINCT order_id), 2) as completion_rate_pct
FROM ecommerce_silver.orders 

UNION ALL

SELECT 
    'Current Month' as period,
    COUNT(DISTINCT order_id) as total_orders,
    ROUND(SUM(total_amount), 2) as total_revenue,
    COUNT(DISTINCT customer_id) as active_customers,
    ROUND(AVG(total_amount), 2) as avg_order_value,
    ROUND(SUM(total_amount) / COUNT(DISTINCT customer_id), 2) as revenue_per_customer,
    COUNT(DISTINCT CASE WHEN status = 'Delivered' THEN order_id END) as completed_orders,
    ROUND(COUNT(DISTINCT CASE WHEN status = 'Delivered' THEN order_id END) * 100.0 / COUNT(DISTINCT order_id), 2) as completion_rate_pct
FROM ecommerce_silver.orders 
WHERE order_year = YEAR(current_date()) 
    AND order_month = MONTH(current_date())
""")

executive_kpis.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_gold.executive_kpis")

print(f"Executive KPIs: {executive_kpis.count()} records")

# COMMAND ----------

# Monthly trends analysis
monthly_trends = (silver_orders
    .filter(F.col("status") == "Delivered")
    .groupBy("order_year", "order_month")
    .agg(
        F.count("order_id").alias("orders"),
        F.sum("total_amount").alias("revenue"),
        F.countDistinct("customer_id").alias("customers"),
        F.avg("total_amount").alias("avg_order_value")
    )
    .withColumn("month_year", F.concat(F.col("order_year"), F.lit("-"), 
                                     F.lpad(F.col("order_month"), 2, "0")))
    .orderBy("order_year", "order_month")
)

monthly_trends.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_gold.monthly_trends")

print(f"Monthly trends: {monthly_trends.count()} records")

# COMMAND ----------

# Optimize tables for performance
print("Optimizing Gold tables...")

spark.sql("OPTIMIZE ecommerce_gold.sales_dashboard ZORDER BY (order_year, order_month)")
spark.sql("OPTIMIZE ecommerce_gold.customer_segments ZORDER BY (customer_tier)")
spark.sql("OPTIMIZE ecommerce_gold.product_performance ZORDER BY (category)")
spark.sql("OPTIMIZE ecommerce_gold.executive_kpis")
spark.sql("OPTIMIZE ecommerce_gold.monthly_trends ZORDER BY (order_year, order_month)")

print("Gold layer optimization complete!")

# COMMAND ----------

# Display key results
print("GOLD LAYER SUMMARY")
print("=" * 40)

# Show executive KPIs
print("Executive KPIs:")
executive_kpis.show(truncate=False)

print("\nTop Customer Segments by Revenue:")
spark.sql("""
SELECT customer_tier, recency_status, customer_count, total_revenue 
FROM ecommerce_gold.customer_segments 
ORDER BY total_revenue DESC 
LIMIT 5
""").show()

print("\nTop Product Categories by Revenue:")
spark.sql("""
SELECT category, category_revenue, units_sold, profit_margin_pct
FROM ecommerce_gold.product_performance 
ORDER BY category_revenue DESC
""").show()

print("\nRecent Monthly Performance:")
spark.sql("""
SELECT month_year, orders, revenue, customers, avg_order_value
FROM ecommerce_gold.monthly_trends 
ORDER BY order_year DESC, order_month DESC
LIMIT 6
""").show()

print("Gold layer complete!")
print("Next: Run 05_dashboard_queries.sql for business insights")

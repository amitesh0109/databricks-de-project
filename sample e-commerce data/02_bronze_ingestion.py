# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Data Processing

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

print("Starting Silver Layer...")

# COMMAND ----------

# Setup
spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce_silver")
spark.sql("USE ecommerce_silver")

# Load bronze tables
bronze_customers = spark.table("ecommerce_bronze.customers")
bronze_products = spark.table("ecommerce_bronze.products") 
bronze_orders = spark.table("ecommerce_bronze.orders")

# COMMAND ----------

# Clean customers data
silver_customers = (bronze_customers
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("is_active") == True)
    .withColumn("full_name", F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")))
    .withColumn("email_domain", F.split(F.col("email"), "@").getItem(1))
    .select("customer_id", "full_name", "email", "email_domain", "country", "segment")
)

# Some basic validation - remove obviously bad emails
silver_customers = silver_customers.filter(F.col("email").contains("@"))

silver_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_silver.customers")

print(f"Silver customers: {silver_customers.count()} records")

# COMMAND ----------

# Clean products data
silver_products = (bronze_products
    .filter(F.col("product_id").isNotNull())
    .filter(F.col("price") > 0)
    .filter(F.col("is_active") == True)
    .withColumn("price_category", 
        F.when(F.col("price") < 50, "Budget")
        .when(F.col("price") < 200, "Standard") 
        .otherwise("Premium"))
    .select("product_id", "product_name", "category", "price", "price_category")
)

silver_products.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_silver.products")

print(f"Silver products: {silver_products.count()} records")

# COMMAND ----------

# Clean and enrich orders data
silver_orders = (bronze_orders
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("total_amount") > 0)
    .join(silver_customers, "customer_id", "inner")  # Only keep orders with valid customers
    .withColumn("order_year", F.year(F.col("order_date")))
    .withColumn("order_month", F.month(F.col("order_date")))
    .withColumn("revenue_category",
        F.when(F.col("total_amount") < 100, "Small")
        .when(F.col("total_amount") < 500, "Medium")
        .otherwise("Large"))
    .select("order_id", "customer_id", "order_date", "order_year", "order_month", 
            "status", "total_amount", "revenue_category", "country", "segment")
)

silver_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_silver.orders")

print(f"Silver orders: {silver_orders.count()} records")

# COMMAND ----------

# Create customer summary
customer_summary = (silver_orders
    .groupBy("customer_id", "country", "segment")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum("total_amount").alias("total_spent"),
        F.avg("total_amount").alias("avg_order_value"),
        F.max("order_date").alias("last_order_date"),
        F.min("order_date").alias("first_order_date")
    )
    .withColumn("customer_lifetime_value", F.col("total_spent"))
    .withColumn("days_since_last_order", 
        F.datediff(F.current_date(), F.col("last_order_date")))
)

customer_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_silver.customer_summary")

print(f"Customer summary: {customer_summary.count()} records")

# COMMAND ----------

# Basic data quality check
print("Silver layer quality check:")
print(f"Orders with missing customers: {silver_orders.filter(F.col('customer_id').isNull()).count()}")
print(f"Negative order amounts: {silver_orders.filter(F.col('total_amount') < 0).count()}")

print("Silver layer complete")

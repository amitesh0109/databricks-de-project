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
bronze_order_items = spark.table("ecommerce_bronze.order_items")

# COMMAND ----------

# Clean customers data
silver_customers = (bronze_customers
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("is_active") == True)
    .withColumn("full_name", F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")))
    .withColumn("email_domain", F.split(F.col("email"), "@").getItem(1))
    .select("customer_id", "full_name", "email", "email_domain", "country", "city", "segment", "registration_date")
)

# Remove bad emails
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
    .withColumn("profit_margin", 
        F.round((F.col("price") - F.col("cost")) / F.col("price") * 100, 2))
    .select("product_id", "product_name", "category", "subcategory", "price", "cost", 
            "price_category", "profit_margin", "stock_quantity")
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
    .join(silver_customers.select("customer_id", "country", "segment"), "customer_id", "inner")
    .withColumn("order_year", F.year(F.col("order_date")))
    .withColumn("order_month", F.month(F.col("order_date")))
    .withColumn("order_quarter", F.quarter(F.col("order_date")))
    .withColumn("revenue_category",
        F.when(F.col("total_amount") < 100, "Small")
        .when(F.col("total_amount") < 500, "Medium")
        .otherwise("Large"))
    .withColumn("days_to_process", 
        F.when(F.col("status") == "Delivered", F.rand() * 7 + 1).otherwise(None))
    .select("order_id", "customer_id", "order_date", "order_year", "order_month", "order_quarter",
            "status", "payment_method", "subtotal", "shipping_cost", "tax_amount", "total_amount", 
            "revenue_category", "days_to_process", "country", "segment")
)

silver_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_silver.orders")

print(f"Silver orders: {silver_orders.count()} records")

# COMMAND ----------

# Clean order items and join with product info
silver_order_items = (bronze_order_items
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("product_id").isNotNull())
    .filter(F.col("quantity") > 0)
    .join(silver_products.select("product_id", "price", "cost", "price_category"), "product_id", "inner")
    .withColumn("total_cost", F.col("quantity") * F

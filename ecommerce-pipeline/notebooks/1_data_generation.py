# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Data Ingestion

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import *

print("Starting Bronze Layer...")

# COMMAND ----------

# Paths configuration
CONFIG = {
    'source_path': '/tmp/ecommerce_data/raw/',
    'bronze_path': '/tmp/ecommerce_data/bronze/',
    'checkpoint_path': '/tmp/ecommerce_data/checkpoints/'
}

for path in CONFIG.values():
    dbutils.fs.mkdirs(path)

spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce_bronze")
spark.sql("USE ecommerce_bronze")

# COMMAND ----------

# COMMAND ----------

# Basic schemas - just the essential ones
customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("is_active", BooleanType(), True)
])

products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("is_active", BooleanType(), True)
])

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("status", StringType(), True),
    StructField("total_amount", DoubleType(), True)
])

# COMMAND ----------

# Ingest customers
df_customers = (spark.read
                .format("csv")
                .option("header", "true")
                .schema(customers_schema)
                .load(f"{CONFIG['source_path']}customers"))

df_customers = df_customers.withColumn("ingestion_timestamp", F.current_timestamp())

df_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{CONFIG['bronze_path']}customers")

spark.sql(f"""
CREATE TABLE ecommerce_bronze.customers
USING DELTA
LOCATION '{CONFIG['bronze_path']}customers'
""")

print(f"Customers: {df_customers.count()} records")

# COMMAND ----------

# Ingest products
df_products = (spark.read
               .format("csv")
               .option("header", "true")
               .schema(products_schema)
               .load(f"{CONFIG['source_path']}products"))

df_products = df_products.withColumn("ingestion_timestamp", F.current_timestamp())

df_products.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{CONFIG['bronze_path']}products")

spark.sql(f"""
CREATE TABLE ecommerce_bronze.products
USING DELTA
LOCATION '{CONFIG['bronze_path']}products'
""")

print(f"Products: {df_products.count()} records")

# COMMAND ----------

# Ingest orders
df_orders = (spark.read
             .format("csv")
             .option("header", "true")
             .schema(orders_schema)
             .load(f"{CONFIG['source_path']}orders"))

df_orders = df_orders.withColumn("ingestion_timestamp", F.current_timestamp())

df_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{CONFIG['bronze_path']}orders")

spark.sql(f"""
CREATE TABLE ecommerce_bronze.orders
USING DELTA
LOCATION '{CONFIG['bronze_path']}orders'
""")

print(f"Orders: {df_orders.count()} records")

# COMMAND ----------

# Quick data check
print("Data Quality Check:")
print(f"Null customer IDs: {spark.table('ecommerce_bronze.customers').filter(F.col('customer_id').isNull()).count()}")
print(f"Negative prices: {spark.table('ecommerce_bronze.products').filter(F.col('price') < 0).count()}")
print(f"Null order totals: {spark.table('ecommerce_bronze.orders').filter(F.col('total_amount').isNull()).count()}")

# Basic optimization
spark.sql("OPTIMIZE ecommerce_bronze.customers")
spark.sql("OPTIMIZE ecommerce_bronze.products") 
spark.sql("OPTIMIZE ecommerce_bronze.orders")

print("Bronze layer complete")
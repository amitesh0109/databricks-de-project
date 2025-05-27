# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Data Ingestion

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

print("Starting Bronze Layer...")

# COMMAND ----------

# Setup paths and database
CONFIG = {
    'source_path': 'abfss://bronze@<your-storage-account>.dfs.core.windows.net/raw/',
    'bronze_path': 'abfss://bronze@<your-storage-account>.dfs.core.windows.net/delta/',
    'checkpoint_path': 'abfss://bronze@<your-storage-account>.dfs.core.windows.net/checkpoints/'
}

for path in CONFIG.values():
    dbutils.fs.mkdirs(path)

spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce_bronze")
spark.sql("USE ecommerce_bronze")

# COMMAND ----------

# Define schemas
customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("registration_date", StringType(), True),
    StructField("is_active", StringType(), True)
])

products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("subcategory", StringType(), True),
    StructField("price", StringType(), True),
    StructField("cost", StringType(), True),
    StructField("stock_quantity", StringType(), True),
    StructField("is_active", StringType(), True)
])

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("order_timestamp", StringType(), True),
    StructField("status", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("subtotal", StringType(), True),
    StructField("shipping_cost", StringType(), True),
    StructField("tax_amount", StringType(), True),
    StructField("total_amount", StringType(), True),
    StructField("currency", StringType(), True)
])

order_items_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("unit_price", StringType(), True),
    StructField("discount_percent", StringType(), True),
    StructField("line_total", StringType(), True)
])

# COMMAND ----------

# Ingest customers
df_customers = (spark.read
                .format("csv")
                .option("header", "true")
                .schema(customers_schema)
                .load(f"{CONFIG['source_path']}customers"))

df_customers = (df_customers
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("registration_date", F.to_date(F.col("registration_date")))
    .withColumn("is_active", F.col("is_active").cast("boolean"))
)

df_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{CONFIG['bronze_path']}customers")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ecommerce_bronze.customers
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

df_products = (df_products
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("price", F.col("price").cast("double"))
    .withColumn("cost", F.col("cost").cast("double"))
    .withColumn("stock_quantity", F.col("stock_quantity").cast("int"))
    .withColumn("is_active", F.col("is_active").cast("boolean"))
)

df_products.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{CONFIG['bronze_path']}products")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ecommerce_bronze.products
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

df_orders = (df_orders
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("order_date", F.to_date(F.col("order_date")))
    .withColumn("order_timestamp", F.to_timestamp(F.col("order_timestamp")))
    .withColumn("subtotal", F.col("subtotal").cast("double"))
    .withColumn("shipping_cost", F.col("shipping_cost").cast("double"))
    .withColumn("tax_amount", F.col("tax_amount").cast("double"))
    .withColumn("total_amount", F.col("total_amount").cast("double"))
)

df_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{CONFIG['bronze_path']}orders")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ecommerce_bronze.orders
USING DELTA
LOCATION '{CONFIG['bronze_path']}orders'
""")

print(f"Orders: {df_orders.count()} records")

# COMMAND ----------

# Ingest order items
df_order_items = (spark.read
                  .format("csv")
                  .option("header", "true")
                  .schema(order_items_schema)
                  .load(f"{CONFIG['source_path']}order_items"))

df_order_items = (df_order_items
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("double"))
    .withColumn("discount_percent", F.col("discount_percent").cast("double"))
    .withColumn("line_total", F.col("line_total").cast("double"))
)

df_order_items.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{CONFIG['bronze_path']}order_items")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ecommerce_bronze.order_items
USING DELTA
LOCATION '{CONFIG['bronze_path']}order_items'
""")

print(f"Order Items: {df_order_items.count()} records")

# COMMAND ----------

# Data quality checks
print("Data Quality Check:")
print(f"Null customer IDs: {spark.table('ecommerce_bronze.customers').filter(F.col('customer_id').isNull()).count()}")
print(f"Negative prices: {spark.table('ecommerce_bronze.products').filter(F.col('price') < 0).count()}")
print(f"Null order totals: {spark.table('ecommerce_bronze.orders').filter(F.col('total_amount').isNull()).count()}")

# Basic optimization
spark.sql("OPTIMIZE ecommerce_bronze.customers")
spark.sql("OPTIMIZE ecommerce_bronze.products") 
spark.sql("OPTIMIZE ecommerce_bronze.orders")
spark.sql("OPTIMIZE ecommerce_bronze.order_items")

print("Bronze layer complete!")
print("Next: Run 03_silver_processing.py")

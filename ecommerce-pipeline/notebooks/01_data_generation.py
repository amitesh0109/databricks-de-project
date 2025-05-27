# Databricks notebook source
# MAGIC %md
# MAGIC # E-commerce Data Generator

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from pyspark.sql import functions as F

np.random.seed(42)
random.seed(42)

print("Generating E-commerce Data...")

# COMMAND ----------

# Generate customers
def create_customers():
    segments = ['Premium', 'Standard', 'Budget']
    countries = ['US', 'CA', 'UK', 'DE', 'AU']
    
    customers = []
    for i in range(500):
        customer = {
            'customer_id': f"CUST{i+1:04d}",
            'first_name': f"Customer{i+1}",
            'last_name': f"LastName{i+1}",
            'email': f"customer{i+1}@email.com",
            'phone': f"+1-555-{random.randint(1000,9999)}",
            'country': np.random.choice(countries, p=[0.6, 0.1, 0.15, 0.1, 0.05]),
            'city': f"City{random.randint(1,50)}",
            'segment': np.random.choice(segments, p=[0.2, 0.5, 0.3]),
            'registration_date': (datetime.now() - timedelta(days=random.randint(30, 730))).date(),
            'is_active': np.random.choice([True, False], p=[0.85, 0.15])
        }
        customers.append(customer)
    
    return pd.DataFrame(customers)

customers_df = create_customers()
print(f"Generated {len(customers_df)} customers")

# COMMAND ----------

# Generate products
def create_products():
    categories = {
        'Electronics': ['Smartphone', 'Laptop', 'Tablet', 'Headphones'],
        'Clothing': ['T-Shirt', 'Jeans', 'Jacket', 'Shoes'],
        'Home': ['Chair', 'Table', 'Lamp', 'Pillow'],
        'Books': ['Fiction', 'Non-Fiction', 'Tech', 'Biography'],
        'Sports': ['Equipment', 'Clothing', 'Accessories', 'Footwear']
    }
    
    products = []
    product_id = 1
    
    for category, subcategories in categories.items():
        for subcategory in subcategories:
            for i in range(5):
                if category == 'Electronics':
                    price = round(random.uniform(100, 1500), 2)
                elif category == 'Clothing':
                    price = round(random.uniform(25, 200), 2)
                elif category == 'Home':
                    price = round(random.uniform(50, 500), 2)
                elif category == 'Books':
                    price = round(random.uniform(10, 50), 2)
                else:
                    price = round(random.uniform(30, 300), 2)
                
                product = {
                    'product_id': f"PROD{product_id:04d}",
                    'product_name': f"{category} {subcategory} Model {i+1}",
                    'category': category,
                    'subcategory': subcategory,
                    'price': price,
                    'cost': round(price * random.uniform(0.4, 0.7), 2),
                    'stock_quantity': random.randint(10, 200),
                    'is_active': np.random.choice([True, False], p=[0.9, 0.1])
                }
                products.append(product)
                product_id += 1
    
    return pd.DataFrame(products)

products_df = create_products()
print(f"Generated {len(products_df)} products")

# COMMAND ----------

# Generate orders and order items
def create_orders():
    orders = []
    order_items = []
    
    statuses = ['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled']
    payment_methods = ['Credit Card', 'PayPal', 'Bank Transfer', 'Cash']
    
    active_customers = customers_df[customers_df['is_active'] == True]['customer_id'].tolist()
    active_products = products_df[products_df['is_active'] == True]
    
    for i in range(1000):
        order_id = f"ORD{i+1:06d}"
        customer_id = random.choice(active_customers)
        
        order_date = datetime.now() - timedelta(days=random.randint(1, 90))
        
        days_old = (datetime.now() - order_date).days
        if days_old > 30:
            status = np.random.choice(['Delivered', 'Cancelled'], p=[0.9, 0.1])
        elif days_old > 7:
            status = np.random.choice(['Delivered', 'Shipped'], p=[0.7, 0.3])
        else:
            status = np.random.choice(['Processing', 'Shipped', 'Pending'], p=[0.5, 0.3, 0.2])
        
        num_items = np.random.choice([1, 2, 3, 4], p=[0.4, 0.3, 0.2, 0.1])
        selected_products = active_products.sample(n=min(num_items, len(active_products)))
        
        order_total = 0
        
        for j, (_, product) in enumerate(selected_products.iterrows()):
            quantity = random.randint(1, 5)
            unit_price = product['price']
            
            discount = np.random.choice([0, 0.05, 0.1, 0.15], p=[0.6, 0.2, 0.15, 0.05])
            final_price = unit_price * (1 - discount)
            line_total = quantity * final_price
            order_total += line_total
            
            order_item = {
                'order_id': order_id,
                'product_id': product['product_id'],
                'product_name': product['product_name'],
                'category': product['category'],
                'quantity': quantity,
                'unit_price': round(final_price, 2),
                'discount_percent': round(discount * 100, 1),
                'line_total': round(line_total, 2)
            }
            order_items.append(order_item)
        
        shipping = 0 if order_total > 100 else 10.99
        tax = round(order_total * 0.08, 2)
        final_total = round(order_total + shipping + tax, 2)
        
        order = {
            'order_id': order_id,
            'customer_id': customer_id,
            'order_date': order_date.date(),
            'order_timestamp': order_date,
            'status': status,
            'payment_method': random.choice(payment_methods),
            'subtotal': round(order_total, 2),
            'shipping_cost': shipping,
            'tax_amount': tax,
            'total_amount': final_total,
            'currency': 'USD'
        }
        orders.append(order)
    
    return pd.DataFrame(orders), pd.DataFrame(order_items)

orders_df, order_items_df = create_orders()
print(f"Generated {len(orders_df)} orders")
print(f"Generated {len(order_items_df)} order items")

# COMMAND ----------

# Save as CSV files
base_path = "abfss://bronze@<your-storage-account>.dfs.core.windows.net/raw/"
dbutils.fs.mkdirs(base_path)

def save_as_csv(pandas_df, filename):
    spark_df = spark.createDataFrame(pandas_df)
    spark_df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"{base_path}{filename}")
    print(f"Saved {filename}: {len(pandas_df)} records")

print("Saving datasets...")
save_as_csv(customers_df, "customers")
save_as_csv(products_df, "products") 
save_as_csv(orders_df, "orders")
save_as_csv(order_items_df, "order_items")

# COMMAND ----------

# Summary
total_revenue = orders_df['total_amount'].sum()
avg_order_value = orders_df['total_amount'].mean()

print(f"""
BUSINESS METRICS SUMMARY
========================
Total Revenue: ${total_revenue:,.2f}
Total Orders: {len(orders_df):,}
Average Order Value: ${avg_order_value:.2f}
Active Customers: {len(customers_df[customers_df['is_active'] == True]):,}
Products in Catalog: {len(products_df):,}

Data generation complete!
""")

print("Next: Run 02_bronze_ingestion.py")

-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Business Dashboard - Analytics Queries

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Executive Summary Dashboard

-- COMMAND ----------

-- Executive KPIs Overview
SELECT 
    period,
    total_orders,
    total_revenue,
    active_customers,
    avg_order_value,
    revenue_per_customer,
    completion_rate_pct
FROM ecommerce_gold.executive_kpis
ORDER BY period

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Sales Performance

-- COMMAND ----------

-- Monthly Sales Trend (Last 6 Months)
SELECT 
    month_year,
    orders,
    ROUND(revenue, 2) as revenue,
    customers,
    ROUND(avg_order_value, 2) as avg_order_value
FROM ecommerce_gold.monthly_trends
ORDER BY month_year DESC
LIMIT 6

-- COMMAND ----------

-- Sales by Status and Revenue Category
SELECT 
    status,
    SUM(order_count) as total_orders,
    ROUND(SUM(revenue), 2) as total_revenue,
    ROUND(AVG(avg_order_value), 2) as avg_order_value
FROM ecommerce_gold.sales_dashboard
GROUP BY status
ORDER BY total_revenue DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Customer Analytics

-- COMMAND ----------

-- Customer Segmentation Summary
SELECT 
    customer_tier,
    recency_status,
    SUM(customer_count) as customers,
    ROUND(SUM(total_revenue), 2) as revenue,
    ROUND(AVG(avg_customer_value), 2) as avg_customer_value,
    ROUND(AVG(avg_orders_per_customer), 1) as avg_orders_per_customer
FROM ecommerce_gold.customer_segments
GROUP BY customer_tier, recency_status
ORDER BY revenue DESC

-- COMMAND ----------

-- Top Customer Segments by Value
SELECT 
    CONCAT(customer_tier, ' - ', recency_status) as segment,
    customer_count,
    ROUND(total_revenue, 2) as revenue,
    ROUND(revenue_per_customer, 2) as revenue_per_customer
FROM ecommerce_gold.customer_segments
WHERE customer_count > 5
ORDER BY total_revenue DESC
LIMIT 10

-- COMMAND ----------

-- Customer Distribution by Country and Segment
SELECT 
    country,
    segment,
    SUM(customer_count) as customers,
    ROUND(SUM(total_revenue), 2) as revenue
FROM ecommerce_gold.customer_segments
GROUP BY country, segment
ORDER BY revenue DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Product Performance

-- COMMAND ----------

-- Product Category Performance
SELECT 
    category,
    ROUND(category_revenue, 2) as revenue,
    units_sold,
    orders_with_category as orders,
    ROUND(revenue_per_order, 2) as revenue_per_order,
    ROUND(profit_margin_pct, 1) as profit_margin_pct,
    ROUND(avg_discount_percent, 1) as avg_discount_pct
FROM ecommerce_gold.product_performance
ORDER BY category_revenue DESC

-- COMMAND ----------

-- Price Category Analysis
SELECT 
    price_category,
    COUNT(*) as categories,
    ROUND(SUM(category_revenue), 2) as total_revenue,
    SUM(units_sold) as total_units,
    ROUND(AVG(profit_margin_pct), 1) as avg_profit_margin
FROM ecommerce_gold.product_performance
GROUP BY price_category
ORDER BY total_revenue DESC

-- COMMAND ----------

-- Top Performing Categories by Profit
SELECT 
    category,
    ROUND(category_revenue, 2) as revenue,
    ROUND(category_profit, 2) as profit,
    ROUND(profit_margin_pct, 1) as profit_margin_pct
FROM ecommerce_gold.product_performance
WHERE category_profit > 0
ORDER BY category_profit DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Business Insights

-- COMMAND ----------

-- Revenue Growth Analysis
WITH monthly_growth AS (
  SELECT 
    month_year,
    revenue,
    LAG(revenue) OVER (ORDER BY month_year) as prev_month_revenue
  FROM ecommerce_gold.monthly_trends
)
SELECT 
  month_year,
  ROUND(revenue, 2) as revenue,
  ROUND(prev_month_revenue, 2) as prev_month_revenue,
  ROUND(((revenue - prev_month_revenue) / prev_month_revenue * 100), 2) as growth_pct
FROM monthly_growth
WHERE prev_month_revenue IS NOT NULL
ORDER BY month_year DESC

-- COMMAND ----------

-- Customer Lifetime Value Distribution
SELECT 
  CASE 
    WHEN avg_customer_value < 100 THEN 'Under $100'
    WHEN avg_customer_value < 300 THEN '$100-$300'
    WHEN avg_customer_value < 600 THEN '$300-$600'
    ELSE 'Over $600'
  END as clv_range,
  SUM(customer_count) as customers,
  ROUND(SUM(total_revenue), 2) as revenue,
  ROUND(AVG(avg_customer_value), 2) as avg_clv
FROM ecommerce_gold.customer_segments
GROUP BY 1
ORDER BY avg_clv DESC

-- COMMAND ----------

-- Order Completion Analysis by Month
SELECT 
    month_year,
    SUM(CASE WHEN status = 'Delivered' THEN order_count ELSE 0 END) as delivered_orders,
    SUM(order_count) as total_orders,
    ROUND(SUM(CASE WHEN status = 'Delivered' THEN order_count ELSE 0 END) * 100.0 / SUM(order_count), 2) as completion_rate
FROM ecommerce_gold.sales_dashboard
GROUP BY month_year
ORDER BY month_year DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Performance Summary

-- COMMAND ----------

-- Overall Business Health Check
SELECT 
  'Total Customers' as metric,
  COUNT(DISTINCT customer_id) as value
FROM ecommerce_silver.customers

UNION ALL

SELECT 
  'Total Products' as metric,
  COUNT(DISTINCT product_id) as value
FROM ecommerce_silver.products

UNION ALL

SELECT 
  'Total Orders' as metric,
  COUNT(DISTINCT order_id) as value
FROM ecommerce_silver.orders

UNION ALL

SELECT 
  'Total Revenue' as metric,
  CAST(ROUND(SUM(total_amount), 0) as INT) as value
FROM ecommerce_silver.orders

UNION ALL

SELECT 
  'Avg Order Value' as metric,
  CAST(ROUND(AVG(total_amount), 0) as INT) as value
FROM ecommerce_silver.orders

# E-commerce Data Pipeline

End-to-end data engineering pipeline demonstrating medallion architecture with Databricks and Delta Lake.

## 🏗️ Architecture

```
CSV Data → Bronze Layer → Silver Layer → Gold Layer → Dashboard
(Raw)       (Ingested)    (Cleaned)     (Analytics)   (Business)
```

## 📊 Dataset

- **500 customers** with segments and demographics
- **100 products** across 5 categories
- **1,000 orders** with realistic business patterns
- **2,000+ order items** with pricing and discounts

## 🚀 Quick Start

### Prerequisites
- Databricks workspace
- Cluster with DBR 12.0+ 

### Execution Order
```python
# Run notebooks in sequence:
%run ./notebooks/01_data_generation      # ~2 min
%run ./notebooks/02_bronze_ingestion     # ~3 min  
%run ./notebooks/03_silver_processing    # ~2 min
%run ./notebooks/04_gold_analytics       # ~2 min
```

### View Results
```sql
-- Open Databricks SQL:
%run ./notebooks/05_dashboard_queries
```

## 📁 Files

```
ecommerce-pipeline/
├── notebooks/
│   ├── 01_data_generation.py       # Generate sample data
│   ├── 02_bronze_ingestion.py      # Raw data ingestion
│   ├── 03_silver_processing.py     # Data cleaning
│   ├── 04_gold_analytics.py        # Business metrics
│   └── 05_dashboard_queries.sql    # Dashboard queries
└── README.md
```

## 💼 Business Value

- **Customer Segmentation**: VIP, Premium, Standard, Basic tiers
- **Revenue Analytics**: Monthly trends and performance
- **Product Performance**: Category-wise sales analysis
- **Executive KPIs**: Real-time business metrics

## 🛠️ Technical Skills

- **PySpark**: Data transformations and processing
- **Delta Lake**: ACID transactions and optimization
- **Medallion Architecture**: Bronze/Silver/Gold layers
- **Performance Tuning**: Partitioning and Z-ordering
- **Data Quality**: Validation and monitoring

## 📈 Results

| Layer | Records | Purpose |
|-------|---------|---------|
| Bronze | 2,100+ | Raw data ingestion |
| Silver | 1,900+ | Cleaned and joined |
| Gold | 50+ | Aggregated metrics |

**Total Runtime**: ~10 minutes

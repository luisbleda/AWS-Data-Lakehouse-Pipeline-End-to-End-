# AWS Data Engineering Pipeline

## 📌 Overview
End-to-end data pipeline built on AWS simulating an e-commerce data platform.

## 🏗️ Architecture
- Data ingestion via API → S3 (raw layer)
- Processing with PySpark / AWS Glue
- Storage in S3 (processed layer - Parquet)
- Querying using Athena

## ⚙️ Tech Stack
- AWS (S3, Glue, Athena, Lambda)
- PySpark
- Python
- Terraform

## 📊 Data Flow
1. Ingest data from API
2. Store raw JSON in S3
3. Transform with Spark
4. Save as Parquet
5. Query via Athena

## 🚀 How to run
1. Configure AWS credentials
2. Run ingestion script:
   ```bash
   python main.py

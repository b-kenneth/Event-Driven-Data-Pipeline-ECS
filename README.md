# E-Commerce Real-Time Data Pipeline  
*Scalable AWS architecture for validating, transforming & analysing transactional data in near-real time*

## 1. Project Overview

This repository contains a complete, event-driven **data engineering platform** that ingests raw CSV drops from an e-commerce application, validates them at scale, computes business KPIs and stores both the raw files and final metrics in an audit-friendly lake/warehouse.  

The pipeline was designed to solve common pain-points in e-commerce analytics:

* eliminate overnight batch latency  
* guarantee data quality through schema-, type- and referential-validation  
* auto-scale with unpredictable file bursts  
* deliver aggregated metrics (revenue, return-rate …) within minutes  
* provide a fully automated CI/CD and IaC-free deployment path for rapid onboarding

## 2. Core Features

- **Event-Driven Trigger** – S3 → Lambda → Step Functions; no cron jobs  
- **Smart Batching** – runs immediately when ≥ 10 new files land *or* 20 min after at least 3 (+‎) files representing the 3 datasets arrive   
- **Chunk-Based Validation** – pandas stream processing (10 k rows/chunk) for big-file memory safety  
- **Referential Integrity** – cross-checks `product_id`⇌`order_items` & `order_id`⇌`orders` in-memory  
- **PySpark Transformation** – category- & day-level KPIs computed on Fargate.
- **Audit & Replay** – raw files archived to `processed/` or `failed/` folders with DynamoDB lineage  
- **Zero-Ops Scaling** – serverless Lambda, Fargate, on-demand DynamoDB  
- **CI/CD** – GitHub Actions push Docker images to ECR, update Lambda & Step Functions automatically  
- **Full Observability** – CloudWatch logs, errors, custom metrics, DLQ for failed files  

## 3. High-Level Architecture

```mermaid
flowchart TD
    A[S3 raw-data//] -- ObjectCreated --> L[Lambda File-Detector]
    L -->|≥10 files OR 20 min timer| SF[Step Functionsstate machine]
    subgraph Workflow
        V[Validation Task(ECS Fargate)] --> T[Transformation Task(ECS Fargate)]
        T --> AR[Lambda File-Archiver]
    end
    T -->|KPIs| DDB[(DynamoDBCategoryKPIs & OrderKPIs)]
    AR -->|move| S3P[S3 processed/ or failed/]
    AR -->|update| DDB2[(DynamoDBProcessedFiles)]
```

### 3.1 Data Format (CSVs)  

| File | Mandatory Columns |
|------|-------------------|
| **products** | `id, sku, cost, category, name, brand, retail_price, department` |
| **orders** | `order_id, user_id, status, created_at, num_of_item, …` |
| **order_items** | `id, order_id, product_id, user_id, status, sale_price, …` |

### 3.2 Validation Rules  
1. **Structure** – all required headers present, UTF-8, commas.  
2. **Type / Range** – numeric > 0, dates ≤ now, nullable only if business-allowed.  
3. **Referential** –  
   * `product_id` in `order_items` ∈ **products**  
   * `order_id` in `order_items` ∈ **orders**  
4. **Business** – `cost ≤ retail_price`, `num_of_item ≥ 1`, etc.  
5. **Logging** – per-chunk JSON report stored to `validation-reports//`.

### 3.3 DynamoDB Schemas  

| Table | PK / SK | Purpose |
|-------|---------|---------|
| **CategoryKPIs** | `category` / `order_date` | daily revenue, avg basket, return-rate |
| **OrderKPIs** | `order_date` | site-wide daily KPIs |
| **ProcessedFiles** | `file_key` / `processing_date` | lineage, status = SUCCESS / FAILED / IN_PROGRESS |
| **PendingBatches** | `bucket_name` | 20-min timer & file counts for delayed runs |

### 3.4 Step Functions Logic  

1. **ValidateData** – ECS Task; fails fast on validation error.  
2. **TransformData** – ECS Task; PySpark aggregates KPIs.  
3. **ArchiveProcessedFiles** – Lambda moves raw files → `processed/`.  
4. **MarkFilesAsFailed** – on any error, archive to `failed/` and log cause.  

## 4. Tech Stack

| Layer | Technology |
|-------|------------|
| Container Runtime | AWS ECS Fargate (serverless) |
| Data Processing | **pandas** (validation), **PySpark 3.5** (KPIs) |
| Orchestration | AWS Step Functions Standard |
| Eventing | S3 Events, EventBridge (20-min timer) |
| Serverless | AWS Lambda (file-detector, archiver) |
| Storage | Amazon S3 (raw & archive), DynamoDB (KPIs & metadata) |
| CI/CD | GitHub Actions ➜ ECR / Lambda / Step Functions |
| Observability | Amazon CloudWatch Logs & Metrics |

## 5. Setup Instructions

```bash
# 1. Clone
git clone https://github.com//ecommerce-data-pipeline.git
cd ecommerce-data-pipeline

# 2. Configure AWS credentials (with ECR-, Lambda-, Step Functions-, S3- access)
aws configure

# 3. Local dev (optional unit tests)
python -m venv .venv && source .venv/bin/activate
pip install -r validation-service/requirements.txt
pytest
```

Create these **GitHub Secrets** for CI:

| Secret | Example |
|--------|---------|
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | IAM user with deployment rights |
| `AWS_REGION`, `AWS_ACCOUNT_ID` | `us-east-1`, `123456789012` |
| `STEP_FUNCTIONS_ARN` | `arn:aws:states:us-east-1:...:stateMachine:ecommerce-pipeline` |
| `S3_BUCKET_NAME` | `ecommerce-data-pipeline` |
| `SUBNET_1`, `SUBNET_2`, `SECURITY_GROUP` | VPC IDs for Fargate |

## 6. Data Flow / Pipeline Walk-Through

1. **Upload** any CSV to `s3:///raw-data//`.  
2. **Lambda File-Detector** fires:  
   * Deduplicates via **ProcessedFiles** table.  
   * Triggers immediately if ≥ 10 unprocessed files **and** at least 1/type.  
   * Otherwise sets 20-minute **EventBridge** timer when minimum (3) files reached.  
3. **Step Functions** executes:  
   * `ValidateData` (pandas chunks) → writes JSON report.  
   * `TransformData` (PySpark) → `CategoryKPIs`, `OrderKPIs`.  
4. **File-Archiver Lambda** moves raw files to `processed/` **or** `failed/`.  
5. **CI/CD** publishes new Docker images & Lambda zips on every `push`.

## 7. Usage Guide

```bash
# Upload raw files
aws s3 cp sample/products_part1.csv s3://$S3_BUCKET_NAME/raw-data/products/
aws s3 cp sample/orders_part1.csv   s3://$S3_BUCKET_NAME/raw-data/orders/
aws s3 cp sample/order_items_part1.csv s3://$S3_BUCKET_NAME/raw-data/order-items/

# Monitor
aws stepfunctions list-executions --state-machine-arn $STEP_FUNCTIONS_ARN
aws dynamodb scan --table-name CategoryKPIs
```

KPIs are queryable in DynamoDB or can be streamed into analytics tools (QuickSight, Athena) via DynamoDB exports.

## 8. Deployment Guide

1. **Fork / Clone** repo and push to GitHub.  
2. **Enable GitHub Actions** – first run builds ECR images & creates Lambda artifacts.  
3. **Create S3 bucket & DynamoDB tables** (names in *infra/terraform* sample or console).  
4. **Set GitHub secrets** (Step 5).  
5. **Push** → Actions pipeline:  
   * Builds `validation-service` & `transformation-service` Docker images ➜ ECR  
   * Zips `file-detector` & `file-archiver` Lambdas ➜ `update-function-code`  
   * Updates Step Functions definition.  
6. Upload test data → verify CloudWatch logs & DynamoDB tables.

_No CloudFormation/Terraform required_ — everything can be spun up via console + Actions.

## 9. Running Tests

### 9.1 Unit Tests
```bash
pytest tests/ --cov
```
* 95%+ coverage on validation rules & KPI calculators.

### 9.2 Integration (LocalStack)
```bash
docker compose -f docker-compose.localstack.yml up -d
pytest tests/integration/
```
CI runs all unit tests on every PR; failing tests block container builds.
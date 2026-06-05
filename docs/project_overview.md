# AWS Glue Snowflake ETL Pipeline - Project Overview

## Project Objective

The objective of this project is to build an automated ETL pipeline that ingests employee data from Amazon S3, processes it using AWS Glue, stores curated data in Apache Iceberg tables, and loads the final data into Snowflake for reporting and analytics.

## Architecture Flow

S3 → EventBridge → Step Function → Glue Crawler → Glue ETL Job → Iceberg Table → Snowflake

## Components Used

* Amazon S3
* AWS EventBridge
* AWS Step Functions
* AWS Glue Crawlers
* AWS Glue ETL Jobs
* AWS Glue Data Catalog
* Apache Iceberg
* Snowflake
* Amazon SNS
* Amazon CloudWatch

## Workflow

1. Source CSV files are uploaded into the S3 raw bucket.
2. EventBridge detects the file arrival event.
3. EventBridge triggers the Step Function workflow.
4. The Step Function starts the Glue Crawler.
5. The Glue Crawler scans the source files and updates the Glue Data Catalog.
6. After the crawler completes successfully, the Step Function triggers the Glue ETL job.
7. The Glue ETL job reads source data and performs data transformations using PySpark.
8. Apache Iceberg is used to support ACID transactions, schema evolution, and MERGE operations.
9. Processed data is stored in the target Iceberg table.
10. Curated data is loaded into Snowflake tables for reporting and analytics.
11. CloudWatch logs are used for monitoring.
12. SNS notifications are sent when failures occur.

## Key Features

* Incremental data processing using Glue Job Bookmarks
* Data quality validations
* Deduplication logic
* Apache Iceberg integration
* Schema evolution support
* Automated workflow orchestration using Step Functions
* Error notifications using SNS
* Monitoring through CloudWatch

## Business Benefits

* Eliminates manual ETL execution
* Supports scalable data processing
* Reduces duplicate data processing
* Improves data quality and consistency
* Provides reliable reporting data in Snowflake

## Repository Structure

aws-glue-snowflake-etl-de/

├── glue_jobs/
│   └── bookmark_etl_job.py

├── step_functions/
│   └── employee_etl_workflow.json

├── docs/
│   └── project_overview.md

├── architecture/

└── README.md

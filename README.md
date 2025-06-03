# Yelp Big Data ETL & Analytics Platform

A fully automated, production-grade data pipeline that transforms raw Yelp JSON into clean, analytics-ready Parquet data using AWS, Databricks (PySpark), and Neo4j.

**Tech Stack**: PySpark · AWS S3 · Lambda · Glue · Athena · QuickSight · Neo4j · Databricks  
**Key Features**: Modular ETL · Event-Driven Ingestion · Sentiment Analysis · Graph Modeling · Visual Dashboards
![image](https://github.com/user-attachments/assets/63aa793d-4ad8-4687-b871-f73bff2b06ce)

## Table of Contents

1. [Introduction and Background](#1-introduction-and-background)  
2. [AWS and Databricks Setup](#2-aws-and-databricks-setup)  
3. [Databricks ETL and Workflow Architecture](#3-databricks-etl-and-workflow-architecture)  
   - [Business Rules for ETL Processing](#business-rules-for-etl-processing)  
4. [Lambda Function as Orchestration Trigger](#4-lambda-function-as-orchestration-trigger)  
5. [Glue Crawler and Athena Table Creation](#5-glue-crawler-and-athena-table-creation)  
6. [Visualization and Machine Learning](#6-visualization-and-machine-learning)  
7. [Graph Analytics with Neo4j AuraDB](#7-graph-analytics-with-neo4j-auradb)  
8. [Conclusion and Practical Significance](#8-conclusion-and-practical-significance)


---

## 1. Introduction and Background

### 1.1 Introduction

In today’s data-driven business world, companies heavily rely on feedback, reviews, and interactions from users to drive product and service improvement. The Yelp Open Dataset provides publicly available data related to local businesses, including business metadata, user reviews, and social connections. This dataset is extensive, semi-structured (in JSON format), and includes complex nested attributes, making it ideal for real-world big data transformation and analytics use cases.

Our primary goal with this project is to build a **scalable, production-grade ETL pipeline** using **AWS cloud services and Databricks** that transforms this semi-structured Yelp data into analytics-ready tables. This pipeline must be automated, efficient, and capable of supporting both batch and real-time data loads. Once the transformed data is stored in a structured format, it will support querying, visualization, and machine learning use cases.


### 1.2 Background

The Yelp dataset includes several domains such as:

- **Business** — metadata about local businesses (name, location, categories, hours, attributes, etc.)
- **Review** — user reviews for businesses including ratings, text, and feedback metrics (cool, funny, useful)
- **User** — profiles of users, including social connections (friends), elite status, and compliments

While the dataset is rich in content, its JSON format poses significant challenges for direct use in business intelligence platforms or analytical queries. This requires several preprocessing steps:

- Parsing nested JSON structures
- Flattening and cleaning text fields
- Engineering derived features (e.g., sentiment score, weekly open hours, engagement metrics)
- Normalizing data across multiple domains for relational joins

The architecture we’ve designed ensures end-to-end automation:

- A user simply drops a JSON file into an AWS S3 bucket.
- A Lambda function detects the upload and triggers a Databricks ETL job.
- After processing, results are written in Parquet format to another S3 bucket.
- A Glue crawler catalogs the data so it can be queried in Athena and visualized in QuickSight.

This design reflects the kind of automated, cloud-native pipelines used by large tech companies for continuous ingestion and transformation of big data.

### 1.3 Project Overview

The Yelp Big Data ETL & Analytics Project demonstrates how to:

1. Integrate cloud services like **S3, Lambda, Glue, Athena, QuickSight**, and **Databricks**.
2. Automate JSON ingestion and transformation for three domains: Business, Review, and User.
3. Join the processed datasets into a single unified analytics table.
4. Catalog the output into a tabular schema using AWS Glue.
5. Enable data querying through Amazon Athena.
6. Build interactive visualizations in Amazon QuickSight.
7. Support downstream ML use cases directly within Databricks.

The result is a reusable reference architecture for real-time data engineering pipelines that convert raw semi-structured files into clean, powerful data assets ready for analytics and decision-making.


**AWS and Databricks Setup**

The starting point of the project was to establish the cloud infrastructure for storing and processing the data. We began by:

Creating an AWS Account: This enabled access to S3, Lambda, Glue, Athena, and other related services.

Subscribing to Databricks via AWS Marketplace: This provided a collaborative and scalable Spark-based platform to develop and run ETL jobs efficiently.

**Setting up IAM Roles:**

We created IAM roles to allow Databricks to access S3 buckets securely.

The IAM roles were granted the following permissions:

s3:GetObject — to read raw JSON files.

s3:PutObject — to write transformed data.

s3:DeleteObject — to remove outdated or temporary data.

These permissions were applied to both of the following buckets:

yelprawdata (for raw JSON input files)

yelpprocesseddata (for cleaned Parquet output files)

This configuration ensured that Databricks could fully interact with the storage layer—reading raw inputs and writing processed outputs—without exposing static credentials. All access was governed by secure IAM roles attached to the Databricks workspace environment.

**Databricks ETL and Workflow Architecture**

Once IAM and permissions were configured, we proceeded with setting up the Databricks environment for ETL processing. The steps included:

Adding IAM Role to Databricks Workspace Credentials:

The ARN of the IAM role created in AWS was added to the workspace credentials section in Databricks.

We also added the role to the Databricks cluster’s cloud resources to ensure seamless access to S3 during job execution.

**Mounting the S3 Buckets:**

We mounted the raw and processed S3 buckets in each ETL notebook using the dbutils.fs.mount() command.

This allowed us to treat S3 data paths like local filesystems for reading and writing.

**Writing Domain-Specific ETL Scripts:**

We created three dedicated notebooks for:

Business ETL

Review ETL

User ETL

Each notebook reads its respective raw JSON files, performs data cleaning and transformations, and writes the results as Parquet files into the appropriate /mnt/yelpprocesseddata subfolder.

**Creating a Unified Analytics Notebook:**

We wrote a fourth notebook called UnifiedAnalytics.

This notebook joins the outputs of the three domain ETLs into a single analytics-ready DataFrame.

It handles renaming overlapping columns, joining on keys (business_id, user_id), and final transformations.

**Orchestrating ETL Workflows with Databricks Jobs:**

<img width="822" alt="Screenshot 2025-05-16 at 1 55 20 AM" src="https://github.com/user-attachments/assets/14565a6c-6f22-4ec8-8ade-b02d98c743e4" />

We created a multi-task job in the Databricks Jobs UI.

The job includes the four notebooks as tasks:

Business ETL (independent)

Review ETL (independent)

User ETL (independent)

UnifiedAnalytics (dependent on the completion of the first three tasks)

This configuration created a Directed Acyclic Graph (DAG) of dependencies ensuring the UnifiedAnalytics notebook only runs after all domain-specific ETLs have finished successfully.

This modular architecture ensures easy debugging, reusability, and scalability while adhering to modern data engineering best practices.
### Business Rules for ETL Processing

Each ETL script follows clearly defined business rules to ensure the data is clean, usable, and optimized for analytics. Below are human-readable explanations of how we handled the transformation for each domain:

#### Common Rules:
- **Flatten nested structures:** Since the original JSON data included deeply nested fields and arrays, we used PySpark functions to explode arrays and simplify nested structs into a flat table schema.
- **Avoid complex data types:** We intentionally reduced the use of `struct` and `array` columns to ensure the final data is compatible with Athena, QuickSight, and Neo4j.
- **Clean and normalize text fields:** This includes trimming whitespace, converting to lowercase, and standardizing null formats.

#### Business ETL:
- **Removed the `address` column** as it was not necessary for analytics and caused clutter in the schema.
- **Dropped rows with null `business_id`**, as these are critical primary keys for joining and analytics.
- **Filtered out businesses with missing `categories`**, since categories are essential for filtering and grouping businesses.
- **Dropped rows with null `hours`**, ensuring only businesses with defined operating hours are retained for time-based analysis.
- **Checked and documented null counts** across fields before making filtering decisions, ensuring decisions were data-driven.
- **Maintained a flat schema** to simplify joins and ensure compatibility with downstream tools like Glue and Athena.

#### Review ETL:
- **Removed reviews without `user_id` or `business_id`** to ensure each review is joinable and has context.
- **Created `engagement_score`** by summing the `useful`, `funny`, and `cool` metrics to represent overall interaction.
- **Dropped raw feedback columns** (`useful`, `funny`, `cool`) after creating the composite metric to streamline the dataset.
- **Applied VADER sentiment analysis** to generate sentiment scores from review text, enabling polarity-based analytics.
- **Checked for nulls across all columns** to validate and clean the dataset before transformation or joins.

#### User ETL:
- **Calculated `engagement_compliments`** by summing the `useful`, `funny`, and `cool` columns to reflect overall user influence.
- **Dropped unnecessary compliment subfields** and retained only key ones: `compliment_list`, `compliment_writer`, `compliment_note`, and `compliment_photos`.
- **Removed raw interaction columns** (`useful`, `funny`, `cool`) after consolidation.
- **Created a numeric `elite_years_count`** from the string field `elite`, representing how many years the user was an elite member.
- **Validated nulls across the dataset** to ensure no corrupted records are included in downstream processing.

#### UnifiedAnalytics:
- **Joined datasets using `user_id` and `business_id`** as reliable foreign keys to connect user reviews with business metadata.
- **Renamed overlapping column names** between user, review, and business tables to avoid schema conflicts (e.g., `name_user`, `name_business`).
- **Performed left joins** to ensure that no reviews were dropped even if user or business metadata was partially missing.
- **Validated schema consistency** after joining, ensuring compatibility with AWS Glue and Athena by enforcing clean column names and types.
- **Overwrote existing unified table** to keep only the latest, cleanly joined dataset in storage.

**Lambda Function as Orchestration Trigger**

The next step was creating an AWS Lambda function to automate execution of the ETL pipeline based on file uploads to S3. This Lambda function acts as a trigger that initiates the relevant ETL process whenever a new file is added to the yelprawdata bucket.

In this Lambda function, we configured and provided all required details such as:

Databricks token

Databricks job ID

Databricks instance URL

This function is designed to respond to s3:ObjectCreated events. It reads the uploaded file path (key), identifies which domain it belongs to (Business, Review, or User), and dynamically invokes the appropriate ETL task in the Databricks multi-task job.

Additionally, the Lambda function also contains a helper function to invoke the Glue Crawler, which is explained in the next section.

The job execution logic is smartly designed to detect whether enough data is present to trigger the unified analytics task:

For example, uploading a Business file triggers only the Business task.

Uploading a Review or User file later triggers those tasks separately.

Only when all three domains have data will the UnifiedAnalytics task execute, since it depends on the other three tasks.

**One important note on data handling strategy:**

The ETL scripts for Business, Review, and User use append mode when writing Parquet files.

The UnifiedAnalytics script uses overwrite mode to always write a clean, fresh unified dataset.

This approach avoids redundant data generation and ensures that unified analytics are consistently up to date. Even when new domain files are added later, the system reuses existing processed data and regenerates the final joined dataset correctly.

This Lambda-triggered architecture enables completely automated, event-driven ETL execution, significantly reducing the need for manual intervention and ensuring data freshness.

**Glue Crawler and Athena Table Creation**

After completing the ETL processing and writing the unified analytics table in Parquet format to the S3 processed bucket, the next crucial step is to make this data queryable through a database engine. This is achieved by configuring an AWS Glue Crawler.

**Purpose of the Crawler**

The Glue Crawler is responsible for:

Scanning the UnifiedAnalytics Parquet folder in the yelpprocesseddata bucket.

Converting that Parquet data into a structured table format.

Creating and updating the corresponding schema in the Glue Data Catalog.

Making the data accessible for SQL queries in Amazon Athena.

An IAM role should be given to this crawler attaching the plolocies to read from the S3 yelpprocessed data bucket so that it can access the data from the bucket

**Integration with Lambda**

As mentioned earlier, the Lambda function not only triggers Databricks jobs but also includes a small function to invoke the Glue Crawler. This crawler is executed after the ETL process finishes and ensures that the database catalog always reflects the most recent state of the Parquet data.

<img width="1182" alt="Screenshot 2025-05-16 at 1 57 15 AM" src="https://github.com/user-attachments/assets/4ae7e039-d6a3-4cf1-a5aa-602d9f5a819f" />

This means:

Whenever a new object (JSON file) is added to the S3 raw bucket, the ETL pipeline is triggered.

The transformed data is written to the processed bucket.

The crawler scans the updated Parquet data and updates the Athena table with the new schema or records.

**Validation via Athena**

Once the crawler finishes running, you can:

Open Amazon Athena.

Navigate to the database (e.g., yelp_db).

Find the unifiedanalytics table.

Run SQL queries directly to verify and analyze the data.

<img width="1470" alt="Screenshot 2025-05-16 at 1 58 45 AM" src="https://github.com/user-attachments/assets/d7515f71-0bf7-4cd5-8431-59a6a44ce425" />

Example:

SELECT * from unifiedanalytics LIMIT 10;

This crawler-based mechanism ensures:

Real-time visibility of new and updated data.

Up-to-date schema detection when new fields or structures are introduced.

A robust and flexible integration layer between S3, Glue, and Athena.

In summary, the Glue Crawler plays a vital role in turning raw Parquet files into a live, queryable dataset accessible via Athena, making real-time business intelligence possible from continuously updated S3 storage.

**Visualization and Machine Learning**

After setting up the data pipeline and validating the schema using Athena, the next step is to build visualizations and extract insights from the processed data.

**Visualizing with QuickSight**

Open Amazon QuickSight from the AWS Console.

Connect it to Athena as a data source.

Select the unifiedanalytics table from the Athena database.

Create interactive dashboards, KPIs, bar charts, line plots, pie charts, and other visual elements.

Analyze metrics like:

Sentiment trends by city or category

Reviewer activity and engagement scores

Distribution of elite reviewers across locations

<img width="1470" alt="Screenshot 2025-05-16 at 1 59 48 AM" src="https://github.com/user-attachments/assets/d539e511-590f-4174-8ffb-c58a94eeb7c9" />

**Exploratory Data Analysis in Databricks**

Since the same processed data is also accessible in Databricks through mounted S3 paths, we can conduct detailed visual analysis directly in notebooks.

Use Python libraries such as:

matplotlib, seaborn, plotly, pandas, pyspark.sql

Generate distribution plots, histograms, and correlation matrices to explore feature relationships.

**Machine Learning Applications**

With a clean and structured dataset available, you can go beyond analysis and build ML models directly in Databricks:

**Supervised Learning:**

Predict star ratings based on review text or engagement metrics

Classify users as elite or not using historical data

**Unsupervised Learning:**

Cluster businesses by location and performance

Segment users based on review patterns and social connections

This step extends the utility of the pipeline, transforming it from a reporting system into a powerful machine learning foundation.

In summary, the data can now be used across two platforms — QuickSight for business visualizations and Databricks for advanced ML and analysis — providing full flexibility and utility for both technical and non-technical stakeholders.
## 7. Graph Analytics with Neo4j AuraDB

To support graph-based analysis and relationship modeling beyond tabular reporting, a parallel pipeline was built using Neo4j AuraDB.

### Why Graph? 
Traditional tabular data structures struggle to efficiently model relationships between entities—like users and businesses—when the connections themselves are important. A graph database such as Neo4j:

- Enables fast traversal of relationships, such as friend-of-a-friend or category-based review networks.
- Allows for graph algorithms like centrality, community detection, and pathfinding, which are useful for influencer analysis, recommendation engines, and localized trends.
- Reflects real-world connections in a more intuitive and expressive way.

### GCP Workaround for AuraDB
Due to Neo4j AuraDB’s restriction on direct access to AWS S3, micro-batch CSVs of cleaned Yelp datasets were exported to Google Cloud Storage (GCS). These were then accessed by Cypher scripts using `LOAD CSV` to ingest into AuraDB.

### Schema Highlights
- **Nodes:**
  - `User`: Yelp users
  - `Business`: Local businesses
  - `Review`: Written reviews
  - `Category`, `City`, `State`, `Feature`: Lookup and dimension nodes

- **Relationships:**
  - `WROTE`: Connects users to the reviews they authored
  - `REVIEWS`: Connects reviews to the businesses they describe
  - `LOCATED_IN`, `IN_STATE`: Link businesses to their geographic locations
  - `HAS_CATEGORY`, `OFFERS`: Describe business metadata and services
  - `FRIENDS_WITH`: Represents bidirectional user connections

### Constraints and Design Rules
- **Uniqueness Constraints:** Applied to all key node IDs (e.g., `user_id`, `business_id`, `review_id`) to avoid duplicate entities.
- **Helper Constraints:** Applied to names in lookup tables (e.g., `Category`, `City`) to prevent duplication.
- **Batch Processing:** Used `apoc.periodic.iterate` for high-volume ingestion to avoid memory issues.
- **Merge Semantics:** Every node and relationship uses `MERGE` instead of `CREATE` to enforce idempotency and ensure no duplicates.
- **Separation of Concerns:** Lookup nodes (cities, states, categories) were separated to support analytic flexibility and avoid redundancy.

### Cypher Script
A full graph loading schema can be found in: [`neo4j_load/graph.cypher`](neo4j_load/graph.cypher)

This script:
- Creates constraints for key nodes
- Ingests CSVs as nodes and relationships
- Enables graph querying for community detection, centrality, sentiment analysis, etc.

## 8. Conclusion and Practical Significance

This project demonstrates the design and execution of a fully automated, scalable, and production-grade big data pipeline for transforming and analyzing semi-structured data at scale. By leveraging the Yelp Open Dataset, we built a cloud-native architecture that integrates AWS services (S3, Lambda, Glue, Athena, QuickSight), Databricks (for Spark-based ETL), and Neo4j AuraDB (for graph analytics).

The solution showcases the following:

- **Automation and Modularity:** The entire process—from file ingestion to analytics—is orchestrated with Lambda functions and modular ETL scripts. This allows for easy maintenance, extensibility, and real-time processing.
- **Robust Data Transformation:** Complex nested JSONs were flattened, normalized, and enriched with engineered features such as sentiment scores and engagement metrics.
- **Interoperability Across Tools:** Data was made queryable in Athena for BI via QuickSight, processed in Spark via Databricks for ML, and modeled in Neo4j for graph analytics.
- **Cross-Cloud Adaptation:** The project highlights a creative solution to a platform limitation by exporting data to GCP for ingestion into Neo4j AuraDB, which lacks native S3 access.

### Business Value

This pipeline design mirrors real-world enterprise practices, where heterogeneous data sources, platform interoperability, and automation are critical. It supports downstream use cases such as:

- Sentiment and customer engagement analytics
- Elite user segmentation and loyalty tracking
- Category-based performance and competitor benchmarking
- Graph-based recommendations and community analysis

By unifying different layers of data engineering, analytics, and graph modeling, this project stands as a robust blueprint for scalable, modern data infrastructure.


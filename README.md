
# 🗜️⚙️ DEB Application Repository 🔩🧰

![Airflow Image Build](https://github.com/uche-madu/deb-application/actions/workflows/retag.yaml/badge.svg)
![Push Pyspark Scripts to GCS](https://github.com/uche-madu/deb-application/actions/workflows/upload_to_gcs.yaml/badge.svg)

***This is one of two repositories with code for the entire DEB Project. While this repository focuses on the application code such as Airflow DAGs, the [DEB Infrastructure repository](https://github.com/uche-madu/deb-infrastructure) focuses on provisioning cloud resources. This separation of concerns via separate repositories aims to follow GitOps Principles.***

# 🎥 Movie Analytics Data Platform 🎬

This repository is part of the **Automated and Scalable Data Platform** project, developed as a capstone for the **Google Africa Data Engineering Bootcamp (Q3-2023)**. The project implements an end-to-end data pipeline to analyze user behavior in the context of movie reviews, leveraging modern data engineering tools and cloud platforms.

---

## **Key Note**

This application repository focuses solely on the **Airflow application** for running the data pipelines. Airflow is deployed on **Google Kubernetes Engine (GKE)** using the **Kubernetes Executor** for scalability and performance.

The required infrastructure (e.g., Kubernetes cluster, GCS buckets, BigQuery datasets) is provisioned via the [DEB Infrastructure Repository](https://github.com/uche-madu/deb-infrastructure).

---

## **Project Overview**

### **Goal**

The goal of this project is to design and implement an end-to-end solution for collecting, processing, and analyzing movie review data, providing actionable insights through a robust data pipeline. The project involves integrating multiple data sources, applying business rules, and delivering insights via an OLAP-ready data warehouse.

### **Key Objectives**

1. **Data Integration**: Collect data from external vendors and internal sources, including:
   - User purchase records from PostgreSQL.
   - Daily review data in CSV files.
   - Log session metadata in CSV files.

2. **Data Processing**:
   - Clean and transform raw data.
   - Classify sentiments in movie reviews using a custom Hugging Face model.
   - Generate dimensional and fact tables in BigQuery for downstream analytics.

3. **Pipeline Orchestration**:
   - Automate ETL/ELT workflows using Apache Airflow.

4. **Business Insights**:
   - Create an OLAP-ready `fact_movie_analytics` table for querying insights such as:
     - Device usage trends.
     - Geographic distribution of reviews.
     - Sentiment analysis outcomes.

---

## **Architecture**

### **Components**

1. **Orchestration**: Apache Airflow manages and schedules all pipeline tasks.
2. **Data Processing**:
   - PySpark on Dataproc for large-scale transformations.
   - Sentiment analysis using a fine-tuned BERT model.
3. **Storage**:
   - Google Cloud Storage (GCS) for raw and staged data.
   - BigQuery for structured and analytical data.
4. **Data Modeling**: dbt (data build tool) for dimensional modeling.
5. **CI/CD**: GitHub Actions for automated deployments.
6. **Infrastructure as Code**: Terraform for provisioning GCP resources.

### **Pipeline Flow**

1. **Raw Data Ingestion**:
   - Load user purchase data into PostgreSQL.
   - Upload movie reviews and log session data to GCS.

2. **Data Processing**:
   - Transform data using PySpark.
   - Perform sentiment analysis on reviews.

3. **Staging and Modeling**:
   - Load cleaned data into BigQuery staging tables.
   - Generate dimensional and fact tables using dbt.

4. **Insights Delivery**:
   - Expose analytical queries through the `fact_movie_analytics` table.

---

## **Tech Stack**

- **Programming Languages**: Python, SQL
- **Data Orchestration**: Apache Airflow
- **Data Processing**: PySpark, Dataproc
- **Data Modeling**: dbt
- **Storage**: Google Cloud SQL (PostgreSQL), GCS, BigQuery
- **Infrastructure as Code**: Terraform
- **CI/CD**: GitHub Actions
- **Sentiment Analysis**: Hugging Face BERT model

---

## **How to Run**

### **CI/CD Workflow**

The deployment and management of the application are automated through a GitHub Actions workflow. The steps are outlined below:

1. **Build and Push Custom Airflow Image**:
   - The `build-push.yaml` workflow builds a custom Airflow image and pushes it to **Google Artifact Registry**.
   - The workflow then checks out the [DEB Infrastructure repository](https://github.com/uche-madu/deb-infrastructure) and updates the `airflow-helm/values-dev.yaml` file with the new image and tag.

2. **Secrets Management**:
   - Configure GitHub secrets for:
     - Your GitHub email and username.
     - A **Personal Access Token** for repository-scoped access.
     - Necessary credentials for GCP (Workload Identity Federation).

3. **Trigger the Workflow**:
   - Commit changes to the `main` or feature branch and push them to GitHub. The CI/CD workflow will automatically deploy the updated Airflow instance and pipeline.

4. **Infrastructure Setup**:
   - The [DEB Infrastructure repository](https://github.com/uche-madu/deb-infrastructure) manages the underlying cloud resources using Terraform.

5. **Access the Airflow UI**:
   - Navigate to the external IP of the Airflow webserver as displayed in the Kubernetes Services list or via the `airflow-webserver` endpoint in the GCP console.

---

## **Analytics**

### **Overview**

The analytics aspect of this project is centered on transforming raw user data, movie reviews, and session logs into actionable insights stored in an OLAP-ready `fact_movie_analytics` table. This table is designed to support business intelligence tools and ad hoc queries for decision-making.

---

### **Fact Table: `fact_movie_analytics`**

The `fact_movie_analytics` table is the core analytical table in this pipeline. It aggregates metrics such as review scores, purchase amounts, and session details. The schema is defined as follows:

| **Column Name**    | **Description**                                                                                   |
|---------------------|---------------------------------------------------------------------------------------------------|
| `customerid`        | Unique ID of the customer (from user purchase data).                                              |
| `id_dim_devices`    | Foreign key linking to the `dim_devices` table.                                                   |
| `id_dim_location`   | Foreign key linking to the `dim_location` table.                                                  |
| `id_dim_os`         | Foreign key linking to the `dim_os` table.                                                        |
| `amount_spent`      | Total amount spent by the customer (calculated as `quantity * unit_price`).                       |
| `review_score`      | Aggregate sentiment score of the customer’s reviews.                                              |
| `review_count`      | Total number of reviews written by the customer.                                                  |
| `insert_date`       | Timestamp of when the record was inserted into the fact table.                                    |

---

### **Dimension Tables**

The fact table references several dimension tables to provide additional context. These tables include:

#### **1. `dim_date`**

| **Column Name** | **Description**                                         |
|------------------|---------------------------------------------------------|
| `id_dim_date`    | Surrogate key for the dimension.                        |
| `log_date`       | Date of the review session.                             |
| `day`            | Day extracted from `log_date`.                          |
| `month`          | Month extracted from `log_date`.                        |
| `year`           | Year extracted from `log_date`.                         |
| `season`         | Season inferred from the `log_date`.                    |

#### **2. `dim_devices`**

| **Column Name** | **Description**                                         |
|------------------|---------------------------------------------------------|
| `id_dim_devices` | Surrogate key for the dimension.                        |
| `device`         | Type of device used for the review (e.g., mobile).      |

#### **3. `dim_location`**

| **Column Name** | **Description**                                         |
|------------------|---------------------------------------------------------|
| `id_dim_location` | Surrogate key for the dimension.                       |
| `location`        | Geographic location of the reviewer.                  |

#### **4. `dim_os`**

| **Column Name** | **Description**                                         |
|------------------|---------------------------------------------------------|
| `id_dim_os`      | Surrogate key for the dimension.                        |
| `os`             | Operating system of the device (e.g., Windows).         |

#### **5. `dim_browser`**

| **Column Name** | **Description**                                         |
|------------------|---------------------------------------------------------|
| `id_dim_browser` | Surrogate key for the dimension.                        |
| `browser`        | Browser used for the review session.                   |

---

### **Key Metrics**

The following are some example metrics and insights that can be derived from the data:

1. **Review Activity**:
   - Total number of reviews across various locations.
   - Device-specific review activity in different regions.

2. **Geographic Analysis**:
   - States with the highest and lowest review activity.
   - Distribution of reviews by device type in different U.S. regions.

3. **Sentiment Insights**:
   - Overall positive vs. negative sentiment trends.
   - Most positively reviewed locations or devices.

4. **Spending Patterns**:
   - Correlation between spending patterns and review sentiment.
   - Total revenue generated by customers who left positive reviews.

---

### **Example Queries**

Here are a few sample SQL queries for deriving insights:

#### **1. Reviews by State and Device**

```sql
SELECT 
    l.location AS state, 
    d.device, 
    COUNT(f.review_count) AS total_reviews
FROM fact_movie_analytics f
JOIN dim_location l ON f.id_dim_location = l.id_dim_location
JOIN dim_devices d ON f.id_dim_devices = d.id_dim_devices
WHERE l.location IN ('California', 'New York', 'Texas')
GROUP BY l.location, d.device
ORDER BY total_reviews DESC;
```

#### **2. Positive Reviews by Location**

```sql
SELECT 
    l.location AS state,
    SUM(f.review_score) AS positive_reviews
FROM fact_movie_analytics f
JOIN dim_location l ON f.id_dim_location = l.id_dim_location
GROUP BY l.location
ORDER BY positive_reviews DESC;
```

#### **3. Revenue and Sentiment Analysis**

```sql
SELECT 
    f.customerid,
    SUM(f.amount_spent) AS total_spent,
    SUM(f.review_score) AS total_positive_reviews
FROM fact_movie_analytics f
GROUP BY f.customerid
ORDER BY total_spent DESC;
```

#### **5. Device Popularity by Region**

```sql
SELECT 
    l.location AS region,
    d.device,
    COUNT(f.review_count) AS review_count
FROM fact_movie_analytics f
JOIN dim_location l ON f.id_dim_location = l.id_dim_location
JOIN dim_devices d ON f.id_dim_devices = d.id_dim_devices
GROUP BY l.location, d.device
ORDER BY review_count DESC;
```

#### **6. Highest Spending Customers**

```sql
SELECT 
    f.customerid,
    SUM(f.amount_spent) AS total_spent
FROM fact_movie_analytics f
GROUP BY f.customerid
ORDER BY total_spent DESC
LIMIT 10;
```

#### **7. Positive Review Trends Over Time**

```sql
SELECT 
    d.year,
    d.month,
    SUM(f.review_score) AS positive_reviews
FROM fact_movie_analytics f
JOIN dim_date d ON f.insert_date = d.log_date
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
```

#### **8. State-Wise Average Spending**

```sql
SELECT 
    l.location AS state,
    AVG(f.amount_spent) AS avg_spending
FROM fact_movie_analytics f
JOIN dim_location l ON f.id_dim_location = l.id_dim_location
GROUP BY l.location
ORDER BY avg_spending DESC;
```

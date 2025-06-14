# 🚀 GCP E-commerce Data Pipeline

This repository contains the code and configurations for an end-to-end batch processing data pipeline on Google Cloud Platform (GCP). This pipeline ingests raw e-commerce data, transforms it through **bronze** and **silver** layers, and prepares a curated **gold** layer for analytical consumption using various GCP services and modern data tools.

---

## 🎯 Introduction

This project aims to build a robust and scalable data pipeline for e-commerce data. It demonstrates a **medallion architecture** (Bronze, Silver, Gold layers) for data processing, ensuring data quality, consistency, and analytical readiness. The pipeline is designed for **batch ingestion**, handling both historical data loads and scheduled daily or periodic updates.

---

## 🗺️ Architecture

The data pipeline follows a medallion architecture pattern (Bronze, Silver, Gold) implemented on GCP.

* **Raw Data Ingestion:**
    * Simulated raw e-commerce data (customers, products, orders, order items, payments) is generated.
    * **Crucially:** In a production environment, this raw data would be continuously produced and land directly in **Google Cloud Storage (GCS)** ☁️ buckets. The CSV files in `data_samples/raw` are solely for local demonstration and showcasing the data format.
* **Bronze Layer (Raw Storage) 🥉:**
    * Raw data from GCS is ingested into the Bronze layer using **Dataproc PySpark** jobs.
    * Data is stored in **Parquet format** in dedicated GCS buckets, maintaining the original structure.
    * Schemas for the Bronze layer are defined in `bigquery_models/bronze/bq_schemas`.
* **Silver Layer (Cleaned & Conformed) 🥈:**
    * Bronze layer data is processed, cleaned, and transformed into a conformed schema using **Dataproc PySpark** jobs.
    * This stage performs data quality checks, data type conversions, and basic aggregations.
    * The cleaned data is stored in **Parquet format** in GCS.
    * Schemas for the Silver layer are defined in `bigquery_models/silver/bq_schemas`.
* **Gold Layer (Curated for Analytics) 🥇:**
    * The Silver layer data is further transformed and aggregated using **dbt (data build tool)**.
    * Analytical-ready tables (e.g., `dim_customers`, `fact_orders`) are created in **BigQuery** 📈.
    * dbt models ensure data consistency, lineage, and facilitate complex analytical queries.
* **Orchestration:**
    * **Apache Airflow** (deployed on Google Cloud Composer 🎶) orchestrates the entire pipeline.
    * DAGs trigger Cloud Functions for data generation, submit Dataproc jobs, and run dbt transformations.
* **Data Generation (Simulated) ⚙️:**
    * **Google Cloud Functions** ☁️ are used to generate synthetic dimensional (customers, dates, products) and transactional (orders, order items, payments) data. This simulates data arriving in the raw landing zone (GCS).
* **Data Insights & Visualization 📊:**
    * **Metabase** is used for creating interactive dashboards and extracting insights from the Gold layer BigQuery tables.

**High-Level Flow:**

Below is a visual representation of the data flow through the pipeline's different stages and key GCP services.

```text
graph TD
    CF[Cloud Functions<br>(Simulated Data Production)] --> GCS_Raw[GCS<br>Raw Landing Zone]
    GCS_Raw --> Dataproc_Bronze[Dataproc<br>(Bronze Ingestion)]
    Dataproc_Bronze --> GCS_Bronze[GCS<br>Bronze Layer]
    GCS_Bronze --> Dataproc_Silver[Dataproc<br>(Silver Processing)]
    Dataproc_Silver --> GCS_Silver[GCS<br>Silver Layer]
    GCS_Silver --> DBT[dbt<br>(Gold Transformation)]
    DBT --> BigQuery_Gold[BigQuery<br>Gold Layer]
    BigQuery_Gold --> Metabase[Metabase<br>(Dashboards)]
    Airflow[Apache Airflow<br>(Orchestration)] --- CF
    Airflow --- Dataproc_Bronze
    Airflow --- Dataproc_Silver
    Airflow --- DBT
```
---

## 🏗️ Project Structure

The repository is organized to clearly separate concerns and stages of the data pipeline.

```text
├── LICENSE                          # 📄 Project license information
├── README.md                        # 📖 This README file
├── .git/                            # 🌳 Git version control directory
├── .idea/                           # ⚙️ IDE-specific (e.g., PyCharm) project configurations
├── bigquery_models/                 # 🏛️ BigQuery table schema definitions (JSON)
│   ├── bronze/                      # Bronze layer schemas
│   │   └── bq_schemas/              # JSON schema files for bronze tables
│   └── silver/                      # Silver layer schemas
│       └── bq_schemas/              # JSON schema files for silver tables
├── cloud_functions/                 # ☁️ Google Cloud Functions for data generation
│   ├── generate_dim_data_cf/        # Cloud Function for dimension data generation
│   └── generate_fact_data_cf/       # Cloud Function for fact data generation
├── config/                          # 📝 Configuration files for the pipeline parameters
├── dags/                            # 🎶 Apache Airflow DAGs for orchestration
├── dataproc_jobs/                   # 💿 PySpark jobs for Dataproc clusters
│   ├── bronze/                      # PySpark scripts for bronze ingestion
│   ├── silver/                      # PySpark scripts for silver processing
│   └── utils/                       # Utility scripts for Dataproc jobs
├── data_insights_dashboards/        # 📊 **Metabase Dashboard Configurations & Exports.** Essential for recreating pre-built dashboards.
├── data_samples/                    # 🧪 Sample data for local testing and demonstration
│   ├── bronze/                      # Sample Parquet data in bronze format
│   ├── raw/                         # Sample raw CSV data
│   └── silver/                      # Sample Parquet data in silver format
├── GCP key/                         # 🔑 Service account key (for demonstration/development ONLY - handle securely in production!)
├── gold_layer_dbt/                  # ✨ dbt project for Gold layer transformations
│   ├── models/                      # dbt SQL models for creating gold tables
│   └── target/                      # dbt compilation and run artifacts
├── jars/                            # 📦 Java Archive (JAR) files for Spark dependencies (e.g., BigQuery connector)
└── scripts/                         # 📜 Utility scripts for deployment/running jobs

```

## ☁️ GCP Services Used

This project leverages the following Google Cloud Platform services:

* **Google Cloud Storage (GCS)**: Scalable and durable object storage for raw, bronze, and silver data layers. Acts as our data lake.
* **Google Cloud Dataproc**: A managed Spark and Hadoop service used for large-scale batch data processing (ingestion, cleaning, and basic transformations).
* **Google Cloud Functions**: Serverless compute platform used to trigger and execute the simulated raw data generation.
* **Google BigQuery**: A serverless, highly scalable, and cost-effective multi-cloud data warehouse used to store the curated Gold layer for analytical queries.
* **Google Cloud Composer (Apache Airflow)**: A fully managed Airflow service that orchestrates the entire pipeline workflow, scheduling jobs and managing dependencies.
* **IAM (Identity and Access Management)**: For managing fine-grained permissions and access control across all GCP resources.

---

## 🛠️ Key Technologies

Beyond GCP services, this project utilizes the following key technologies:

* **Python**: The primary programming language for PySpark jobs and Cloud Functions.
* **Apache Spark**: The distributed processing engine (via Dataproc) for handling large datasets.
* **dbt (data build tool)**: An open-source tool for data modeling and transformation, used to build the Gold layer in BigQuery.
* **Apache Airflow**: An open-source platform to programmatically author, schedule, and monitor workflows.
* **Metabase**: An open-source business intelligence tool for creating interactive dashboards and extracting insights.
* **Git**: For version control of the entire codebase.

## 🚀 Setup and Deployment

This section guides you through setting up your GCP environment and deploying the pipeline components.

### Prerequisites

Before you begin, ensure you have:

* **GCP Account:** With billing enabled.
* **Google Cloud SDK (gcloud CLI):** Installed and configured on your local machine.
* **Terraform (Optional but Recommended):** For managing GCP infrastructure as code.
* **Python 3.8+:** Installed locally.
* **Poetry (Recommended):** For Python dependency management.
* **Docker & Docker Compose:** Required to run Metabase locally for development and testing.

### Google Cloud Setup

1.  **Create a GCP Project:**
    ```bash
    gcloud projects create YOUR_GCP_PROJECT_ID
    gcloud config set project YOUR_GCP_PROJECT_ID
    ```
2.  **Enable APIs:** Enable the following APIs in your GCP project:
    * Cloud Storage
    * Dataproc
    * Cloud Functions
    * BigQuery
    * Cloud Composer (Airflow)
    * IAM
3.  **Create GCS Buckets:** Create dedicated GCS buckets for raw, bronze, silver data, and a staging bucket for Dataproc jobs.
    ```bash
    gsutil mb gs://your-project-raw-data-bucket
    gsutil mb gs://your-project-bronze-data-bucket
    gsutil mb gs://your-project-silver-data-bucket
    gsutil mb gs://your-project-dataproc-staging-bucket
    ```
4.  **Create Service Accounts:**
    * Create a service account for **Dataproc** with roles like `Dataproc Worker`, `BigQuery Data Editor`, `Storage Object Admin`.
    * Create a service account for **Cloud Functions** with `Cloud Functions Invoker`, `Storage Object Creator`.
    * Create a service account for **Cloud Composer** with necessary roles for interacting with GCS, Dataproc, and BigQuery.
    * ⚠️ **Security Note:** The `GCP key/ecommerce-batch-pipeline-dbt.json` file is provided *solely for quick setup in a development environment*. For production, it's **highly recommended** to use [Workload Identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity) (for GKE/Composer) or rely on default service account credentials where applicable. Manage secrets securely (e.g., Google Secret Manager) instead of embedding them in the repository.
5.  **Create a Dataproc Cluster:**
    ```bash
    gcloud dataproc clusters create your-dataproc-cluster-name \
        --region=your-gcp-region \
        --zone=your-gcp-zone \
        --master-machine-type=n1-standard-4 \
        --worker-machine-type=n1-standard-4 \
        --num-workers=2 \
        --image-version=2.1-debian11 \
        --bucket=your-project-dataproc-staging-bucket \
        --service-account=your-dataproc-service-account@YOUR_GCP_PROJECT_ID.iam.gserviceaccount.com \
        --properties=spark:spark.jars.packages=com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.35.0
    ```
6.  **Set up Cloud Composer Environment:**
    * Follow the GCP documentation to create a Cloud Composer environment in your chosen region.
    * Ensure the Composer environment's service account has permissions to trigger Cloud Functions, submit Dataproc jobs, and interact with BigQuery.

### Local Setup

1.  **Clone the Repository:**
    ```bash
    git clone [https://github.com/your-username/gcp-ecommerce-data-pipeline.git](https://github.com/your-username/gcp-ecommerce-data-pipeline.git)
    cd gcp-ecommerce-data-pipeline
    ```
2.  **Install Python Dependencies:**
    * For `cloud_functions/` and `dataproc_jobs/`: Refer to their respective `requirements.txt` files for dependencies.
    * It's highly recommended to set up Python virtual environments for local development (`python -m venv .venv` and `source .venv/bin/activate`).

### Deployment Steps

1. **Upload Dataproc Job Scripts and Jars to GCS:**
    ```bash
    gsutil cp -r dataproc_jobs/* gs://your-project-dataproc-staging-bucket/dataproc_jobs/
    gsutil cp jars/* gs://your-project-dataproc-staging-bucket/jars/
    ```
2. **Deploy Cloud Functions:**
    * For `generate_dim_data_cf`:
        ```bash
        gcloud functions deploy generate_dim_data_cf \
            --runtime python310 \
            --trigger-http \
            --entry-point main \
            --source cloud_functions/generate_dim_data_cf \
            --allow-unauthenticated \ # Adjust as needed for production security
            --set-env-vars BUCKET_NAME=your-project-raw-data-bucket
        ```
    * For `generate_fact_data_cf`:
        ```bash
        gcloud functions deploy generate_fact_data_cf \
            --runtime python310 \
            --trigger-http \
            --entry-point main \
            --source cloud_functions/generate_fact_data_cf \
            --allow-unauthenticated \ # Adjust as needed for production security
            --set-env-vars BUCKET_NAME=your-project-raw-data-bucket
        ```
3. **Upload Airflow DAGs to Composer Bucket:**
    ```bash
    # Get your Composer DAGs bucket path from the Composer environment details (e.g., gs://your-composer-env-bucket/dags)
    gsutil cp dags/* gs://your-composer-dags-bucket/dags/
    ```
4. **Configure dbt:**
    * Navigate to the `gold_layer_dbt/` directory.
    * Configure your `profiles.yml` to connect to your BigQuery project. This file is typically located in `~/.dbt/profiles.yml` or can be specified via the `DBT_PROFILES_DIR` environment variable.
        ```yaml
        # Example ~/.dbt/profiles.yml entry
        gold_layer_dbt:
          target: dev
          outputs:
            dev:
              type: bigquery
              method: service-account
              project: YOUR_GCP_PROJECT_ID
              dataset: gold_dataset # Name of your BigQuery dataset for gold layer
              keyfile: /path/to/your/service_account_key.json # ⚠️ Reference your key securely; DO NOT commit this file directly.
              threads: 4
              timeout_seconds: 300
              location: US # or your BigQuery dataset location (e.g., asia-south1)
              priority: interactive
        ```
        **Important:** Never commit your service account key file directly to Git. Use environment variables or secure secret management for production environments.
5. **Run dbt initially (optional, or let Airflow handle it):**
    ```bash
    cd gold_layer_dbt
    dbt debug
    dbt deps
    dbt seed # If you have seed files
    dbt run
    dbt test
    ```
6. **Set up Metabase (Local for Development):**
    ```bash
    cd data_insights_dashboards
    docker-compose up -d
    ```
    Access Metabase at http://localhost:3000. Configure a BigQuery connection to your Gold layer dataset (YOUR_GCP_PROJECT_ID.gold_dataset). 
    Crucially: To replicate the pre-built dashboards and KPIs, ensure you keep the data_insights_dashboards/ folder and its contents intact. 
    These files contain the necessary Metabase configurations and exports for easy setup.

---

## ▶️ Running the Pipeline

Once deployed, the `ecommerce_batch_ingestion_dag_3.py` DAG in Airflow will orchestrate the full pipeline. You can trigger it manually from the Airflow UI or let its defined schedule run it automatically.

The DAG will typically perform these steps:
1.  Trigger Cloud Functions to generate raw data into your designated GCS raw bucket.
2.  Submit Dataproc jobs to ingest data from GCS Raw to the Bronze layer.
3.  Submit Dataproc jobs to transform data from the Bronze layer to the Silver layer.
4.  Trigger dbt runs to transform data from the Silver layer into the curated Gold layer in BigQuery.

---

## 🔍 Monitoring and Observability

* **Cloud Logging:** Monitor detailed logs from Cloud Functions, Dataproc jobs, and Airflow task executions for troubleshooting.
* **Cloud Monitoring:** Set up custom metrics and alerts for pipeline health, job failures, resource utilization, and data flow anomalies.
* **Airflow UI:** The Cloud Composer (Airflow) UI provides a comprehensive view of DAG runs, task statuses, logs, and historical performance.
* **BigQuery UI:** Directly query and inspect the data in the BigQuery Bronze, Silver, and Gold datasets to verify transformations.

---

## 🧪 Data Samples

The `data_samples/` directory provides illustrative examples of the data at different stages of the pipeline for development and testing purposes:

* `data_samples/raw/`: Example CSV files, mirroring the format of data as it would initially arrive in the GCS raw landing zone.
* `data_samples/bronze/`: Example Parquet files, representing data after raw ingestion into the bronze layer.
* `data_samples/silver/`: Example Parquet files, showing data after cleaning and transformation into the silver layer.

**Note:** These local files are for illustrative purposes. In a real-world scenario, the raw data would be continuously generated and land directly in the designated GCS bucket for processing.

---

## 📊 Dashboards and Insights

The `data_insights_dashboards/` directory contains configurations and exports for Metabase. Once Metabase is running and connected to your BigQuery Gold layer, you can import the provided dashboard JSON (`ecommerce-batch-pipeline-dbt.json`) to visualize key e-commerce metrics and trends.

---

## 👋 Contributing

Contributions are welcome! Please follow these steps:

1.  Fork the repository.
2.  Create a new feature branch (`git checkout -b feature/your-awesome-feature`).
3.  Make your changes.
4.  Commit your changes (`git commit -m 'feat: Add new awesome feature'`).
5.  Push to the branch (`git push origin feature/your-awesome-feature`).
6.  Open a Pull Request to the `main` branch.

---

## ⚖️ License

This project is licensed under the terms defined in the [LICENSE](LICENSE) file.

---
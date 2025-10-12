# Pikcha Analytics

Pikcha Analytics is a comprehensive end-to-end real-time ETL and analytics platform developed for the retail chain “Pikcha”. The platform processes data from a simulated NoSQL storage (representing the customer’s CRM), streams it via Kafka with sensitive data (email, phone) masked, stores raw data in ClickHouse, transforms it using PySpark for advanced analytics, orchestrates workflows with Airflow, and visualizes insights in Grafana dashboards with Telegram notifications for anomalies (e.g., >50% duplicates in data).

**Architecture Schema**:  
![Architecture Schema](screen/Shema.jpg)

## Project Goals
- **Data Generation**: Generate synthetic JSON data for 45 stores (30 “Большая Пикча” >200 sq.m., 15 “Маленькая Пикча” <100 sq.m.), 20+ products across 5 categories (Grain/Bakery, Meat/Fish/Eggs, Dairy, Fruits/Berries, Vegetables/Greens), 45+ customers (at least one per store), and 200+ purchases.
- **Data Ingestion**: Load JSON files into MongoDB (simulating customer’s NoSQL storage) using Python scripts.
- **Streaming & Masking**: Stream data via Kafka, masking sensitive fields (email, phone) before loading into ClickHouse RAW storage.
- **Raw Storage**: Store raw data in ClickHouse with appropriate table schemas for customers, stores, products, and purchases, preserving original data types (e.g., phone as STRING).
- **Data Cleaning**: Implement an ETL pipeline in ClickHouse using Materialized Views to clean data (remove duplicates, nulls, empty strings, invalid dates) and convert to lowercase, creating a MART layer.
- **Feature Engineering**: Compute 30 customer behavior features (e.g., `bought_milk_last_30d`, `recurrent_buyer`, `vegetarian_profile`) using PySpark, stored as CSV in MinIO.
- **Orchestration**: Automate the ETL pipeline with Airflow DAGs, scheduled daily at 10:00.
- **Visualization & Alerting**: Visualize purchase and store metrics in Grafana dashboards and send Telegram alerts for data anomalies (e.g., >50% duplicates).
- **Output Storage**: Export analytics results to MinIO as CSV files (e.g., `analytic_result_2025_08_01.csv`).

## Tech Stack
| Component        | Purpose                           |
|------------------|-----------------------------------|
| Python 3.11     | Data generation and ETL scripts   |
| Airflow         | Workflow orchestration            |
| Kafka + Zookeeper | Streaming data pipeline          |
| ClickHouse      | Analytical data storage (RAW & MART) |
| PySpark         | Feature computation and aggregation |
| MongoDB         | Raw JSON storage (simulated CRM)  |
| MinIO           | Object storage (S3-compatible)    |
| Grafana         | Visualization and alerting        |
| Docker          | Containerization & service management |

## Project Structure
```
pikcha-analytics/
├── data/                         # Generated data
│   ├── customers/                # Customer JSON files
│   ├── products/                 # Product JSON files
│   ├── purchases/                # Purchase JSON files
│   └── stores/                   # Store JSON files
├── docker/                       # Docker configurations
│   ├── airflow/                  # Airflow setup
│   │   ├── dags/                 # Airflow DAGs
│   │   │   └── pikcha_etl_daily.py
│   │   ├── plugins/              # Airflow plugins
│   │   ├── logs/                 # Airflow logs (ignored)
│   │   ├── postgres_data/        # Postgres data (ignored)
│   │   ├── requirements-airflow.txt
│   │   └── Dockerfile.airflow
│   ├── clickhouse/               # ClickHouse setup
│   │   ├── config.xml
│   │   ├── listen.xml
│   │   ├── users.xml
│   │   └── init_clickhouse.sql   # Initializes ClickHouse tables
│   ├── grafana/                  # Grafana setup
│   │   └── provisioning/
│   │       ├── alerting/         # Alert rules
│   │       │   └── config.json
│   │       ├── dashboards/       # Dashboard config
│   │       │   └── dashboard.yml
│   │       └── datasources/      # Datasource config
│   │           └── datasource.yml
│   └── python.Dockerfile         # Kafka/ClickHouse Python image
├── scripts/                      # Scripts for data generation and ETL
│   ├── generate_data.py          # Synthetic data generator
│   ├── load_to_mongo.py          # Load JSON data into MongoDB
│   ├── kafka_publisher.py        # Publish messages to Kafka
│   ├── kafka_to_clickhouse.py    # Stream Kafka → ClickHouse
│   ├── etl_features.py           # Feature computation via PySpark
│   ├── pyspark_etl.py            # ETL pipeline
│   ├── pyspark_check.py          # PySpark checks
│   ├── check_minio.py           # MinIO checks
│   ├── kafka_consume_sample.py   # Kafka consumer sample
│   └── outputs/                  # ETL results (CSV/Parquet)
├── screen/                       # Screenshots for documentation
│   ├── Airflow.png               # Airflow DAG success
│   ├── Minio.png                 # MinIO file upload
│   ├── Shema.jpg                 # Project architecture schema
│   ├── Tg_alert.png              # Telegram alert
│   ├── alert_rules.png           # Grafana alert rules
│   └── dashboard.png             # Grafana dashboard
├── docker-compose.yml            # All services
├── README.markdown               # Project documentation
└── spark-env.cmd                 # Spark environment config
```

## Installation & Setup
1. **Clone the repository**:
   ```bash
   git clone https://github.com/AlexanderFL7/pikcha-analytics.git
   cd pikcha-analytics
   ```

2. **Environment setup**:
   Create a `.env` file in the project root:
   ```
   AIRFLOW_UID=50000
   AIRFLOW_GID=0
   POSTGRES_USER=airflow
   POSTGRES_PASSWORD=airflow
   CLICKHOUSE_USER=default
   CLICKHOUSE_PASSWORD=
   MINIO_ROOT_USER=minio
   MINIO_ROOT_PASSWORD=minio123
   MINIO_BUCKET_NAME=analytics
   KAFKA_BOOTSTRAP=host.docker.internal:9092
   KAFKA_MASK_KEY=my_secret_key
   TELEGRAM_BOT_TOKEN=8094935037:AAEM3EYPy3hmzJtic6nZKKo3iKaGffq09qE
   TELEGRAM_CHAT_ID=428228184
   ```

3. **Launch all services**:
   From the project root:
   ```bash
   docker compose up -d
   docker ps
   ```

   **Services**:
   | Service    | URL                       | Credentials         |
   |------------|---------------------------|---------------------|
   | Airflow    | http://localhost:8080     | admin / admin       |
   | Grafana    | http://localhost:3000     | admin / admin       |
   | ClickHouse | http://localhost:8123     | —                   |
   | MinIO      | http://localhost:9001     | minio / minio123    |
   | MongoDB    | mongodb://localhost:27017 | —                   |
   | Kafka      | host.docker.internal:9092 | —                   |

   The `init_clickhouse.sql` script automatically creates all required tables when ClickHouse starts.

4. **Generate and load data**:
   Activate your Python environment and run:
   ```bash
   python scripts/generate_data.py
   python scripts/load_to_mongo.py
   python scripts/kafka_publisher.py
   python scripts/kafka_to_clickhouse.py
   ```

5. **Run the ETL pipeline**:
   Run manually:
   ```bash
   python scripts/pyspark_etl.py
   ```
   Or use Airflow:
   - Go to http://localhost:8080
   - Enable DAG `pikcha_etl_daily`

   **Airflow DAG Success**:  
   ![Airflow DAG](screen/Airflow.png)

## Grafana Configuration
All Grafana components are provisioned automatically on startup.  
**Provisioned files**:
- Dashboard: `grafana/dashboards/dashboard.json`
- Data source: `grafana/provisioning/datasources/datasource.yml`
- Alerts: `grafana/provisioning/alerting/config.json`

**Check in Grafana**:
- Connections → Data Sources → ClickHouse
- Dashboards → Pikcha Dashboard
- Alerting → Contact points → Telegram Piccha

**Grafana Dashboard**:  
![Grafana Dashboard](screen/dashboard.png)

**Grafana Alert Rules**:  
![Alert Rules](screen/alert_rules.png)

## Telegram Alerts
**To test in Grafana**:
- Alerting → Contact Points → Telegram Piccha → Send Test Notification

When duplicates exceed 50% in any table, a notification is sent to the configured Telegram chat, alerting the team about data quality issues. 
 
**Telegram Alert**:  
![Telegram Alert](screen/Tg_alert.png)

## MinIO File Upload
Processed analytics results are uploaded to MinIO as CSV files (e.g., `analytic_result_2025_08_01.csv`), containing customer behavior features for clustering.  

**MinIO Upload**:  
![MinIO Upload](screen/Minio.png)

## Monitoring Overview
| Component        | Status Check                                    |
|------------------|-------------------------------------------------|
| Airflow          | DAG `pikcha_etl_daily` active                   |
| Kafka            | Topic `purchases` receives messages             |
| ClickHouse       | Tables created from `init_clickhouse.sql`       |
| Grafana          | Dashboard displays live metrics                 |
| Telegram         | Alerts successfully sent                        |

## Dashboard Overview
The dashboard visualizes:
- Total purchases (200) and stores (45) to confirm data volume
- Total purchases and revenue by store
- Top 10 products by sales
- Purchase trends by day
- Customer activity metrics
- Region-based analytics

## Documentation
- [Airflow Documentation](https://airflow.apache.org/docs/)
- [ClickHouse Documentation](https://clickhouse.com/docs/)
- [Grafana Documentation](https://grafana.com/docs/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)

## Author
- **Name**: Alexander Fedchuk
- **Email**: al-fedchuk@yandex.ru
- **Telegram**: @F_AlexanderL
- **GitHub**: @AlexanderFL7
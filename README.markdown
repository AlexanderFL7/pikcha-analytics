# 🧠 Pikcha Analytics

**Pikcha Analytics** is a complete end-to-end **real-time ETL and analytics platform** for a retail chain “Pikcha”.  
It handles data generation, streaming via Kafka, transformation in PySpark, analytics in ClickHouse, orchestration in Airflow, and visualization & alerting in Grafana with Telegram notifications.

---

## 🚀 Project Goals

- Generate synthetic data for customers, purchases, stores, and products.  
- Stream and mask sensitive data (emails, phones) via **Kafka**.  
- Store and analyze data in **ClickHouse** (raw + clean layers).  
- Compute advanced features with **PySpark** and export to **MinIO**.  
- Visualize insights in **Grafana** dashboards.  
- Automate all pipelines using **Airflow DAGs**.  
- Send alerts to **Telegram** when anomalies are detected.

---

## 🧩 Tech Stack

| Component | Purpose |
|------------|----------|
| **Python 3.11** | Data generation and ETL scripts |
| **Airflow** | Workflow orchestration |
| **Kafka + Zookeeper** | Streaming data pipeline |
| **ClickHouse** | Analytical data storage |
| **PySpark** | Feature computation and aggregation |
| **MongoDB** | Raw JSON storage (simulated CRM) |
| **MinIO** | Object storage (S3-compatible) |
| **Grafana** | Visualization and alerting |
| **Docker** | Containerization & service management |

---

## 📁 Project Structure

pikcha-analytics/
├── clickhouse/
│ └── init_clickhouse.sql # Initializes ClickHouse tables automatically
│
├── dags/
│ └── etl_dag.py # Airflow DAG for daily ETL process
│
├── data/ # Generated data
│ ├── customers/
│ ├── products/
│ ├── purchases/
│ └── stores/
│
├── docker/
│ ├── Dockerfile.airflow # Airflow custom image
│ └── python.Dockerfile # Kafka/ClickHouse Python image
│
├── docs/
│ └── run_instructions.md # Setup documentation
│
├── etl/
│ ├── pyspark_etl.py # Feature computation via PySpark
│ └── requirements.txt # Dependencies
│
├── generator/
│ ├── generate_data.py # Synthetic data generator
│ └── sample_products.json # Product template
│
├── grafana/
│ ├── dashboards/
│ │ └── dashboard.json # Exported Grafana dashboard
│ └── provisioning/
│ ├── dashboards/
│ │ └── dashboard.yml # Dashboard provisioning
│ ├── datasources/
│ │ └── datasource.yml # ClickHouse datasource config
│ └── alerting/
│ └── contact-points.yml # Telegram alert contact point
│
├── loader/
│ ├── load_to_nosql.py # Load JSON data into MongoDB
│ ├── kafka_publisher.py # Publish messages to Kafka
│ └── kafka_to_clickhouse.py # Stream Kafka → ClickHouse
│
├── outputs/ # ETL results (CSV/Parquet)
│
├── docker-compose.yml # All services (root level)
│
└── README.md

yaml
Копировать код

---

## ⚙️ Installation & Setup

### 1. Clone the repository
```bash
git clone https://github.com/AlexanderFL7/pikcha-analytics.git
cd pikcha-analytics
2. Environment setup
Create a .env file in the project root:

bash
Копировать код
AIRFLOW_UID=50000
AIRFLOW_GID=0
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=admin123
MINIO_BUCKET_NAME=analytics
KAFKA_BOOTSTRAP=host.docker.internal:9092
KAFKA_MASK_KEY=my_secret_key
TELEGRAM_BOT_TOKEN=8094935037:AAEM3EYPy3hmzJtic6nZKKo3iKaGffq09qE
TELEGRAM_CHAT_ID=428228184
3. Launch all services
From the project root:

bash
Копировать код
docker compose up -d
docker ps
Services:

Service	URL	Credentials
Airflow	http://localhost:8080	airflow / airflow
Grafana	http://localhost:3000	admin / Grafana1122
ClickHouse	http://localhost:8123	—
MinIO	http://localhost:9001	admin / admin123
MongoDB	mongodb://localhost:27017	—
Kafka	host.docker.internal:9092	—

The init_clickhouse.sql script automatically creates all necessary tables when the ClickHouse container starts.

4. Generate and load data
Activate your Python environment and run:

bash
Копировать код
python generator/generate_data.py
python loader/load_to_nosql.py
python loader/kafka_publisher.py
python loader/kafka_to_clickhouse.py
5. Run the ETL pipeline
bash
Копировать код
python etl/pyspark_etl.py
Or use Airflow:

Go to http://localhost:8080

Enable DAG pikcha_etl_daily

📊 Grafana Configuration
All Grafana components are provisioned automatically on startup.

Provisioned files:

Dashboard: grafana/dashboards/dashboard.json

Data source: grafana/provisioning/datasources/datasource.yml

Alerts: grafana/provisioning/alerting/contact-points.yml

Check in Grafana:

⚙️ Connections → Data Sources → ClickHouse

📊 Dashboards → Pikcha Dashboard

🔔 Alerting → Contact points → Telegram Piccha

🔔 Telegram Alerts
contact-points.yml:

yaml
Копировать код
apiVersion: 1
contactPoints:
  - orgId: 1
    name: Telegram Piccha
    receivers:
      - uid: ddbb27c3-310a-4e31-b58e-7ca014cb8755
        type: telegram
        settings:
          bottoken: ${TELEGRAM_BOT_TOKEN}
          chatid: "${TELEGRAM_CHAT_ID}"
          message: "🚨 Attention! Duplicates >50% detected in table {{ index .CommonLabels \"table\" }}"
To test:

Grafana → Alerting → Contact Points → Telegram Piccha → Send Test Notification

🧠 Monitoring Overview
Component	Status Check
Airflow	DAG pikcha_etl_daily active
Kafka	Topic purchases receives messages
ClickHouse	Tables auto-created from init_clickhouse.sql
Grafana	Dashboard renders live metrics
Telegram	Alert notifications received

📈 Dashboard Overview
Dashboard panels include:

Total purchases and revenue by store

Top 10 products by sales

Purchase trends by day

Customer activity metrics

Region-based analytics

📚 Documentation
Setup guide: docs/run_instructions.md

Airflow Docs

ClickHouse Docs

Grafana Docs

Kafka Docs

PySpark Docs

👤 Author
Alexander Fedchuk
📧 al-fedchuk@yandex.ru
💬 Telegram: @F_AlexanderL
GitHub: @AlexanderFL7
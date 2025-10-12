# Pikcha Analytics

Pikcha Analytics is a complete end-to-end real-time ETL and analytics platform for the retail chain “Pikcha”. It handles data generation, streaming via Kafka, transformation in PySpark, analytics in ClickHouse, orchestration in Airflow, and visualization and alerting in Grafana with Telegram notifications.

## Project Goals
- Generate synthetic data for customers, purchases, stores, and products
- Stream and mask sensitive data (emails, phones) via Kafka
- Store and analyze data in ClickHouse (raw + clean layers)
- Compute advanced features using PySpark and export to MinIO
- Visualize insights in Grafana dashboards
- Automate pipelines using Airflow DAGs
- Send alerts to Telegram when anomalies are detected

## Tech Stack
| Component        | Purpose                           |
|------------------|-----------------------------------|
| Python 3.11     | Data generation and ETL scripts   |
| Airflow         | Workflow orchestration            |
| Kafka + Zookeeper | Streaming data pipeline          |
| ClickHouse      | Analytical data storage           |
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

## Screenshots
Below are key visuals of the project in action:

- **Architecture Schema**:  
  ![Architecture Schema](screen/Shema.jpg)
- **Airflow DAG Success**:  
  ![Airflow DAG](screen/Airflow.png)
- **Grafana Dashboard**:  
  ![Grafana Dashboard](screen/dashboard.png)
- **Grafana Alert Rules**:  
  ![Alert Rules](screen/alert_rules.png)
- **MinIO File Upload**:  
  ![MinIO Upload](screen/Minio.png)
- **Telegram Alert**:  
  ![Telegram Alert](screen/Tg_alert.png)

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
   MINIO_ROOT_USER=admin
   MINIO_ROOT_PASSWORD=admin123
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
   | Airflow    | http://localhost:8080     | airflow / airflow    |
   | Grafana    | http://localhost:3000     | admin / Grafana1122 |
   | ClickHouse | http://localhost:8123     | —                   |
   | MinIO      | http://localhost:9001     | admin / admin123    |
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

## Telegram Alerts
**Example configuration** (`grafana/provisioning/alerting/config.json`):
```yaml
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
          message: "Attention! Duplicates >50% detected in table {{ index .CommonLabels \"table\" }}"
```

**To test in Grafana**:
- Alerting → Contact Points → Telegram Piccha → Send Test Notification

## Monitoring Overview
| Component        | Status Check                                    |
|------------------|-------------------------------------------------|
| Airflow          | DAG `pikcha_etl_daily` active                   |
| Kafka            | Topic `purchases` receives messages             |
| ClickHouse       | Tables created from `init_clickhouse.sql`       |
| Grafana          | Dashboard displays live metrics                 |
| Telegram         | Alerts successfully sent                        |

## Dashboard Overview
The dashboard includes:
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
# Аналитика Пикчи

Проект ETL для обработки и анализа данных сети магазинов «Пикча» в реальном времени. Включает генерацию тестовых данных, потоковую обработку через Kafka, аналитику в ClickHouse, визуализацию в Grafana и оркестрацию через Airflow.

🚀 **Цели проекта**:
- Генерация синтетических данных (покупатели, покупки, товары, магазины).
- Потоковая обработка через Kafka с маскировкой PII (email, phone).
- Аналитика в ClickHouse с сырыми и чистыми таблицами, материализованными представлениями.
- Построение признаков с помощью PySpark, выгрузка в MinIO.
- Визуализация в Grafana.
- Оркестрация ETL через Airflow.
- Алертинг через Telegram.

## 🛠 Технологический стек

### Основные компоненты
- **Airflow** — оркестрация ETL.
- **ClickHouse** — аналитика.
- **PySpark** — обработка данных, вычисление признаков.
- **Kafka + Zookeeper** — потоковая передача.
- **MongoDB** — хранение JSON (симуляция клиентской базы).
- **MinIO** — S3-хранилище.
- **Grafana** — визуализация.

### Инфраструктура
- **Docker** — контейнеризация.
- **Python 3.11** — скрипты для генерации и обработки данных.
- **Git** — управление кодом.

## 📁 Структура проекта

```
pikcha-analytics/
├── clickhouse/                    # SQL-скрипты для ClickHouse
│   ├── raw_tables.sql            # Сырые таблицы
│   ├── mart_mv.sql               # Чистые таблицы и MV
│   └── alerts.sql                # Запросы для алертов
├── docker/                       # Конфигурация Docker
│   ├── docker-compose.yml        # Сервисы
│   └── Dockerfile.airflow        # Airflow Dockerfile
├── docs/                         # Документация
│   └── run_instructions.md       # Инструкции
├── etl/                          # ETL-скрипты
│   ├── pyspark_etl.py            # Вычисление признаков
│   └── requirements.txt          # Зависимости
├── generator/                    # Генерация данных
│   ├── generate_data.py          # Генератор JSON
│   └── sample_products.json      # Пример продуктов
├── grafana/                      # Конфигурация Grafana
│   └── piccha-dashboard.json     # Дашборд
├── infra/grafana/provisioning/   # Настройки Grafana
├── loader/                       # Загрузка данных
│   ├── load_to_nosql.py         # JSON в MongoDB
│   ├── kafka_publisher.py        # Публикация в Kafka
│   └── kafka_to_clickhouse.py    # Kafka в ClickHouse
├── dags/                         # DAG'и Airflow
│   └── etl_dag.py               # Ежедневный ETL
├── data/                         # Данные
│   ├── stores/                  # Магазины
│   ├── products/                # Товары
│   ├── customers/               # Покупатели
│   └── purchases/               # Покупки
├── outputs/                      # Результаты ETL (CSV)
└── README.md                     # Документация
```

## 🛠 Установка и запуск

### Требования
- **Git**
- **Python 3.11**
- **Docker Desktop** (с WSL 2)
- **Java 17** (для PySpark)
- **MongoDB Compass** (опционально)

### 1. Клонирование репозитория
```bash
git clone https://github.com/AlexanderFL7/pikcha-analytics.git
cd pikcha-analytics
```

### 2. Установка Python
1. Установи Python 3.11: [Скачать](https://www.python.org/downloads/windows/). Включи **Add Python to PATH**.
2. Проверь:
   ```powershell
   python --version
   pip --version
   ```
3. Создай и активируй окружение:
   ```powershell
   python -m venv venv311
   .\venv311\Scripts\Activate.ps1
   ```
4. Установи зависимости:
   ```powershell
   pip install -r etl/requirements.txt
   pip install kafka-python clickhouse-connect pymongo
   ```

### 3. Установка Java
1. Установи OpenJDK 17: [Скачать](https://adoptium.net/temurin/releases/?version=17).
2. Настрой `JAVA_HOME`:
   ```powershell
   $Env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17"
   $Env:Path += ";$Env:JAVA_HOME\bin"
   ```
3. Проверь:
   ```powershell
   java -version
   ```

### 4. Установка Docker
1. Установи Docker Desktop: [Скачать](https://www.docker.com/products/docker-desktop/). Включи WSL 2.
2. Установи WSL 2:
   ```powershell
   wsl --install
   ```
3. Проверь:
   ```powershell
   docker --version
   docker-compose --version
   ```

### 5. Настройка окружения
Создай файл `.env`:
```bash
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

### 6. Запуск сервисов
```powershell
cd docker
docker-compose up -d
docker ps
```

### 7. Порты и доступ
- **Airflow**: `http://localhost:8080` (airflow/airflow)
- **ClickHouse**: `http://localhost:8123`, Native: `9000`
- **Grafana**: `http://localhost:3000` (admin/Grafana1122)
- **MinIO**: `http://localhost:9001` (admin/admin123)
- **MongoDB**: `mongodb://localhost:27017`
- **Kafka**: `host.docker.internal:9092`

### 8. Генерация данных
```powershell
.\venv311\Scripts\Activate.ps1
python generator/generate_data.py
```

### 9. Загрузка данных
1. В MongoDB:
   ```powershell
   python loader/load_to_nosql.py
   ```
2. В Kafka:
   ```powershell
   python loader/kafka_publisher.py
   ```
3. В ClickHouse:
   ```powershell
   python loader/kafka_to_clickhouse.py
   ```

### 10. Создание таблиц в ClickHouse
```powershell
Get-Content clickhouse/raw_tables.sql | docker exec -i docker-clickhouse-1 clickhouse-client --multiquery
Get-Content clickhouse/mart_mv.sql | docker exec -i docker-clickhouse-1 clickhouse-client --multiquery
curl "http://localhost:8123/?query=SHOW%20TABLES%20FROM%20piccha"
```

### 11. Запуск ETL
```powershell
$Env:PYSPARK_PYTHON = "C:\Users\Sanya\Desktop\pikcha-analytics\venv311\Scripts\python.exe"
$Env:PYSPARK_DRIVER_PYTHON = "C:\Users\Sanya\Desktop\pikcha-analytics\venv311\Scripts\python.exe"
python etl/pyspark_etl.py
```

### 12. Настройка Grafana
1. Войди: `http://localhost:3000` (admin/Grafana1122).
2. Добавь ClickHouse:
   - Server: `clickhouse`
   - Port: `8123`
   - Protocol: `HTTP`
   - Username: `default`
   - Database: `piccha`
3. Импортируй `grafana/piccha-dashboard.json`.

### 13. Настройка Airflow
1. Войди: `http://localhost:8080` (airflow/airflow).
2. Включи DAG `pikcha_etl_daily`.

### 14. Алертинг
1. В Grafana создай Contact Point (Telegram):
   - Bot Token: `8094935037:AAEM3EYPy3hmzJtic6nZKKo3iKaGffq09qE`
   - Chat ID: `428228184`
2. Настрой Alert Rules (см. `clickhouse/alerts.sql`).

## 📊 Мониторинг и визуализация
- **Grafana**: `piccha-dashboard.json`:
  - Количество покупок и магазинов.
  - Динамика покупок по дням.
  - Топ-10 магазинов по выручке.
- **MinIO**: Результаты в `analytics`.
- **Telegram**: Алерты о дубликатах (>50%).

## 📝 Документация
- Инструкции: `docs/run_instructions.md`
- [Airflow](https://airflow.apache.org/docs/)
- [ClickHouse](https://clickhouse.com/docs/)
- [Kafka](https://kafka.apache.org/documentation/)
- [PySpark](https://spark.apache.org/docs/latest/api/python/)
- [Grafana](https://grafana.com/docs/)
- [MinIO](https://min.io/docs/)
- [MongoDB](https://www.mongodb.com/docs/)

## 🤝 Вклад в проект
1. Форкни: `https://github.com/AlexanderFL7/pikcha-analytics`
2. Создай ветку: `git checkout -b feature/your-feature`
3. Зафиксируй: `git commit -m "Add your feature"`
4. Отправь: `git push origin feature/your-feature`
5. Создай Pull Request.

## 📄 Лицензия
MIT. См. `LICENSE`.

## 📧 Контакты
- **Автор**: Федчук Александр Леонидович (@AlexanderFL7)
- **Email**: [al-fedchuk@yandex.ru](mailto:al-fedchuk@yandex.ru)
- Создавайте Issues для вопросов.

## 🚧 Известные проблемы
- `KafkaTimeoutError` в `kafka_to_clickhouse.py` (проверить `host.docker.internal:9092`).
- Конфликт порта 5432 для `airflow_postgres`.
- Добавить признаки в `pyspark_etl.py` (например, анализ лояльности).
- Настроить дополнительные дашборды в Grafana.
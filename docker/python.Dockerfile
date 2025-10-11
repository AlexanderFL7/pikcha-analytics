FROM python:3.10-slim

WORKDIR /app

# Устанавливаем зависимости
RUN pip install --no-cache-dir kafka-python clickhouse-connect

# Копируем папку scripts в контейнер
COPY scripts /app/scripts

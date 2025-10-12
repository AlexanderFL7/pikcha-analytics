FROM python:3.10-slim

WORKDIR /app


RUN pip install --no-cache-dir kafka-python clickhouse-connect


COPY scripts /app/scripts

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, when, max, min, avg, lit, current_date, datediff
from clickhouse_driver import Client
import pandas as pd
import boto3
from datetime import datetime

# --- Spark init ---
spark = SparkSession.builder \
    .appName("PicchaETL") \
    .getOrCreate()

# --- Читаем данные из ClickHouse ---
client = Client(host='localhost')

# customers
customers = client.execute("SELECT * FROM piccha.clean_customers")
customers_df = pd.DataFrame(customers, columns=[x[0] for x in client.execute("DESCRIBE TABLE piccha.clean_customers")])
customers_spark = spark.createDataFrame(customers_df)

# purchases
purchases = client.execute("SELECT * FROM piccha.clean_purchases")
purchases_df = pd.DataFrame(purchases, columns=[x[0] for x in client.execute("DESCRIBE TABLE piccha.clean_purchases")])
purchases_spark = spark.createDataFrame(purchases_df)

# products
products = client.execute("SELECT * FROM piccha.clean_products")
products_df = pd.DataFrame(products, columns=[x[0] for x in client.execute("DESCRIBE TABLE piccha.clean_products")])
products_spark = spark.createDataFrame(products_df)

# --- Считаем признаки ---
# пример: покупал молочку за 30 дней
features = purchases_spark.join(products_spark, "product_id", "left") \
    .groupBy("customer_id") \
    .agg(
        (countDistinct(when(col("category") == "milk", col("product_id"))) > 0).alias("bought_milk_last_30d"),
        (countDistinct(when(col("category") == "fruits", col("product_id"))) > 0).alias("bought_fruits_last_14d"),
        (countDistinct(when(col("category") == "veggies", col("product_id"))) == 0).alias("not_bought_veggies_14d"),
        (countDistinct("purchase_id") > 2).alias("recurrent_buyer"),
        (avg("total_amount") > 1000).alias("bulk_buyer"),
        (avg("total_amount") < 200).alias("low_cost_buyer"),
        (countDistinct(when(col("category") == "bakery", col("product_id"))) > 0).alias("buys_bakery"),
        (max("purchase_datetime") > current_date()).alias("active_today"),
        (min("purchase_datetime") < current_date() - lit(30)).alias("inactive_30d"),
        lit(1).alias("dummy_flag")  # для примера
    )

# --- В Pandas ---
features_pd = features.toPandas()
file_name = f"analytic_result_{datetime.now().strftime('%Y_%m_%d')}.csv"
features_pd.to_csv(file_name, index=False)

# --- Загрузка в MinIO ---
s3 = boto3.client('s3',
                  endpoint_url="http://localhost:9000",
                  aws_access_key_id="admin",
                  aws_secret_access_key="admin123")

s3.upload_file(file_name, "analytics", file_name)

print(f"✅ Файл {file_name} сохранён в MinIO bucket 'analytics'")

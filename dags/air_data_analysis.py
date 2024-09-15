from pyspark.sql import SparkSession
from pyspark.sql.functions import * 


data_path = "/opt/airflow/datasets/Airbnb.csv"
spark = SparkSession.builder \
    .master('local[*]') \
    .appName("Airbnb_regr_model") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Подключение к таблице
df = spark.read.csv(data_path, header=True, inferSchema=True, sep=';', encoding='cp1251')


# Удаление лишнего
df = df.drop('name', 'id', 'host_name', 'last_review',  'host_id', 'reviews_per_month', 'latitude', 'longitude')
df = df.dropna(how='any')
df = df.filter(
    col('availability_365') > 15
)

price_distribution = df.groupBy('neighbourhood_group').agg(
    avg('price').alias('average_price'),
    min('price').alias('min_price'),
    max('price').alias('max_price'),
    count('price').alias('count')
)

price_distribution.write.mode("overwrite").saveAsTable("price_distribution")

print('Таблица price_distribution успешно загружена в Hive!')


spark.sql("""
CREATE TABLE IF NOT EXISTS processed_airbnb_data (
    neighbourhood_group STRING,
    neighbourhood STRING,
    room_type STRING,
    price INT,
    minimum_nights INT,
    number_of_reviews INT,
    calculated_host_listings_count INT,
    availability_365 INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
""")

print('Успешно создана таблица processed_airbnb_data!')

# Записываем данные в таблицу
df.write.mode("overwrite").insertInto("processed_airbnb_data")


print('Данные успешно записаны в таблицу processed_airbnb_data')


from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.regression import LinearRegressionModel

from pyspark.sql.types import *
from pyspark.sql.functions import * 


spark = SparkSession.builder \
    .master('local[*]') \
    .appName("Airbnb_regr_model") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()


# Загружаем модель
model_path = "/opt/airflow/models/airbnb_model"
model = PipelineModel.load(model_path)

print(f"Модель успешно загружена из Airflow по пути: {model_path}")

test_data = spark.read.table("test_data")

predictions = model.transform(test_data)


predictions.printSchema()

predictions = predictions.select(
    "prediction", 
    "price", 
    "neighbourhood_group", 
    "neighbourhood", 
    "room_type", 
    "minimum_nights", 
    "number_of_reviews", 
    "calculated_host_listings_count", 
    "availability_365"
)

predictions.show(2)
predictions.printSchema()
print(type(predictions))

# spark.sql("""
#     CREATE TABLE IF NOT EXISTS price_prediction (
#         prediction DOUBLE,
#         price INT,
#         neighbourhood_group STRING,
#         neighbourhood STRING,
#         room_type STRING,
#         minimum_nights INT,
#         number_of_reviews INT,
#         calculated_host_listings_count INT,
#         availability_365 INT
#     )
# ROW FORMAT DELIMITED
# FIELDS TERMINATED BY ','
# STORED AS TEXTFILE
# """)

# print('Успешно создана таблица price_prediction')

predictions.write.mode("overwrite").saveAsTable("price_prediction")

# predictions.write.mode("overwrite").insertInto("price_prediction")

print('Даг успешно выполнен! Это победа!!!')
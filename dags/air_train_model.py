from pyspark.sql import SparkSession
from pyspark.sql.functions import * 

from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline

spark = SparkSession.builder \
    .master('local[*]') \
    .appName("Airbnb_train_model") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()


# Считывание данных из таблицы
df = spark.read.table("processed_airbnb_data")

df = df.dropna(how='any')


print('Таблица успешно считана!')


# 3. Кодируем категориальные признаки
indexers = [
    StringIndexer(inputCol="neighbourhood_group", outputCol="neighbourhood_group_index", handleInvalid="skip"),
    StringIndexer(inputCol="neighbourhood", outputCol="neighbourhood_index", handleInvalid="skip"),
    StringIndexer(inputCol="room_type", outputCol="room_type_index", handleInvalid="skip")
]

# 4. Векторизация категориальных признаков
encoders = [
    OneHotEncoder(inputCol="neighbourhood_group_index", outputCol="neighbourhood_group_ohe"),
    OneHotEncoder(inputCol="neighbourhood_index", outputCol="neighbourhood_ohe"),
    OneHotEncoder(inputCol="room_type_index", outputCol="room_type_ohe")
]

# 5. Объединение всех признаков в вектор
assembler = VectorAssembler(
    inputCols=["neighbourhood_group_ohe", "neighbourhood_ohe", "room_type_ohe", "minimum_nights", "number_of_reviews",
               "calculated_host_listings_count", "availability_365"],
    outputCol="features"
)

lr = LinearRegression(featuresCol="features", labelCol="price")

# 6. Создаем Pipeline для выполнения всех шагов
pipeline = Pipeline(stages=indexers + encoders + [assembler, lr])

df.printSchema()


# Разделение данных на 80% для тренировки и 20% для теста
train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

train_data.printSchema()


model = pipeline.fit(train_data)

print('Тип данных', type(model))
print(type(pipeline))
print('Модель успешно обучена!')

spark.sql("""
CREATE TABLE IF NOT EXISTS test_data (
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

test_data.write.mode("overwrite").insertInto("test_data")

# Сохраняем обученную модель
model_path = "/opt/airflow/models/airbnb_model"
model.write().overwrite().save(model_path)


print(f"Модель успешно сохранена в Airflow по пути: {model_path}")
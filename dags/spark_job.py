from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col
from pyspark.sql.types import *


# Создаем SparkSession с поддержкой Hive
spark = SparkSession.builder \
    .master('local[*]') \
    .appName("SparkJob") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

class Parameters:
  path_pc = "/opt/airflow/datasets/pc/pc.txt"
  path_product = "/opt/airflow/datasets/product/product.txt"

  table_pc = "pc"
  table_product = "product"

  def createTable(self, name:str, spark:SparkSession, structType: StructType, path:str, delimiter: str = ','):
      spark.read\
            .options(delimiter = delimiter, nullValue = "\\N")\
            .schema(structType)\
            .csv(path)\
            .createOrReplaceTempView(name)
      
  def initTables(self, spark:SparkSession):
    self.createTable(Parameters.table_pc, spark, PC.structType, Parameters.path_pc)
    self.createTable(Parameters.table_product, spark, Product.structType, Parameters.path_product)

class PC:
  structType = StructType([
      StructField("ID", IntegerType()),
      StructField("MODEL", IntegerType()),
      StructField("SPEED", IntegerType()),
      StructField("RAM", IntegerType()),
      StructField("C2", IntegerType()),
      StructField("C3", StringType()),
      StructField("PRICE", IntegerType()),
  ])
  
class Product:
  structType = StructType([
      StructField("Maker", StringType()),
      StructField("Model", StringType()),
      StructField("Type", StringType())
  ])

p = Parameters()
p.initTables(spark)

# Создание таблицы в Hive
# spark.sql("""
#     CREATE TABLE IF NOT EXISTS pc_makers (
#         maker STRING
#     )
    
#     ROW FORMAT DELIMITED
#     FIELDS TERMINATED BY ','
#     STORED AS TEXTFILE;
# """)

# Ваш код
final_table = spark.table("product").alias('p')\
                  .filter(col("p.type") == "PC")\
                  .join(spark.table("pc"), col("p.model") == col("pc.model"), "left")\
                  .groupBy("maker")\
                  .agg(count("p.model").alias("count_p_model"), count("pc.model").alias("count_pc_model"))\
                  .filter(col("count_p_model") == col("count_pc_model"))\
                  .select("maker")


print('Тип данной таблицы - ', type(final_table))
print("Available tables:", spark.catalog.listTables())


# spark.sql("CREATE DATABASE IF NOT EXISTS makers")

# Запись результата в Hive без предварительного создания таблицы
final_table.write.mode("overwrite").saveAsTable("pc_makers")
    

spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.master('local[*]').getOrCreate()

class Parameters:
  path_pc = "datasets/pc/pc.txt"
  path_product = "datasets/product/product.txt"

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
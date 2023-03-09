#necessary libraries of pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from os.path import abspath
import logging


warehouse_location = abspath('spark-warehouse')



if __name__ == '__main__':
#     #Creating spark session
   spark = SparkSession.builder.master("spark://localhost:7077").appName("demo").config("spark.sql.warehouse.dir", warehouse_location).enableHiveSupport().getOrCreate()
    
   # movies_df=spark.read.csv('/movies.dat',header=True)
    
#     movies_df.show()
#     spark.sql("CREATE DATABASE IF NOT EXISTS analytics")
#     movies_df.write.mode('overwrite') \
#          .saveAsTable("analytics.movies")

#     df=spark.read.table("analytics.movies")
#     df.show()


    #  database = "Coviddata" #your database name
    #  collection = "region" #your collection name
    #  connectionString=   ('mongodb+srv://Revanth:cheeku@cluster0.l7yfihd.mongodb.net/?retryWrites=true&w=majority')
    #  spark = SparkSession\
    #  .builder\
    #  .config('spark.mongodb.input.uri',connectionString)\
    #  .config('spark.mongodb.output.uri', connectionString)\
    #  .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1')\
    #  .getOrCreate()\
    #  # Reading from MongoDB
    #  region = spark.read\
    #  .format("com.mongodb.spark.sql.DefaultSource")\
    #  .option("uri", connectionString)\
    #  .option("database", database)\
    #  .option("collection", collection)\
    #  .load()
    #  region.show()

   
    
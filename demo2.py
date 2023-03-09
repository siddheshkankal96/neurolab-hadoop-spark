#necessary libraries of pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,DoubleType


if __name__ == '__main__':
    #Creating spark session
    spark = SparkSession.builder.appName("demo").getOrCreate()


    # df = spark.read.csv('/input_data/demo.csv')
    customSchema = StructType([
    StructField("id", IntegerType(), True),        
    StructField("name", StringType(), True),
    StructField("sal", DoubleType(), True)
])
    df = spark.read.load('/input_data/demo.csv', format="csv", schema=customSchema)

    #Printing data frame schema
    df.printSchema()

    #Printing data
    df.show(truncate=False)

    #Writing file in hadoop
    df.write.csv("/input_data/record.csv")

#necessary libraries of pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import *
from os.path import abspath
import logging


warehouse_location = abspath('spark-warehouse')



if __name__ == '__main__':
#     #Creating spark session
   spark = SparkSession.builder.master("spark://localhost:7077").appName("demo").config("spark.sql.warehouse.dir", warehouse_location).enableHiveSupport().getOrCreate()
    
   # movies_df=spark.read.csv('/movies.csv',header=True) --sai's
   
   # movies_schema = StructField

   movies_schema= StructType([ \
                 StructField("Movie_id", IntegerType(),True),\
                 StructField("title", StringType(),True),\
                 StructField("Movie_genre", StringType(),True)\
             ])
   df = spark.read.format("csv").schema(movies_schema).option("header", "False").option("sep","::").load("/project_data/movies.dat")
   # df.show()
   # df.printSchema()
   # df.show(2,truncate=False)


   # @udf(returnType=ArrayType()) 
   # def split_by_pipe(s):
   #    return s.split('|')

   # movies_df = df.select(col("Movie_id"),col("Movie_name"),\
   #          split_by_pipe(col("Movie_genre")).alias("Movie_genres"),
   #          ) \
   #          .drop("Movie_genre")
   # movies_df.withColumn("Movie_genre", split_by_pipe(col("Movie_genre"))).show(truncate=False)

   movies_df1 = df.select(col("Movie_id"),col("title"),\
            split(col("Movie_genre"),"\\|").alias("Movie_genres"),
            ) \
            .drop("Movie_genre")

   # movies_df.printSchema()
   # movies_df.show(2,truncate=False)

   movies_df1_genre = movies_df1.select(col('movie_id'),col('title'),explode(col('Movie_genres')).alias('genres'))
   # movies_df1_genre.show(2,truncate=False)

   movies_df2_year = movies_df1.withColumn('release_year',regexp_extract(col('title'),"(\\d+)",1))

   # movies_df2_year.show(2,truncate=False)





# --------------------------------------------------
# users  data 

   # UserID::Gender::Age::Occupation::Zip-code


   users_schema= StructType([ \
                 StructField("UserID", IntegerType(),True),\
                 StructField("Gender", StringType(),True),\
                 StructField("Age", IntegerType(),True),\
                 StructField("Occupation", StringType(),True),\
                 StructField("Zip-code", StringType(),True)\
             ])

   users_df = spark.read.format("csv").schema(users_schema).option("header", "False").option("sep","::").load("/project_data/users.dat")


   # users_df.show(2,truncate=False)
	# *  0:  "other" or not specified
	# *  1:  "academic/educator"
	# *  2:  "artist"
	# *  3:  "clerical/admin"
	# *  4:  "college/grad student"
	# *  5:  "customer service"
	# *  6:  "doctor/health care"
	# *  7:  "executive/managerial"
	# *  8:  "farmer"
	# *  9:  "homemaker"
	# * 10:  "K-12 student"
	# * 11:  "lawyer"
	# * 12:  "programmer"
	# * 13:  "retired"
	# * 14:  "sales/marketing"
	# * 15:  "scientist"
	# * 16:  "self-employed"
	# * 17:  "technician/engineer"
	# * 18:  "tradesman/craftsman"
	# * 19:  "unemployed"
	# * 20:  "writer"

   Occupation = { "0" :  "other","1" :  "academic/educator","2" :  "artist","3" :  "clerical/admin","4" :  "college/grad student","5" :  "customer service","6" :  "doctor/health care","7" :  "executive/managerial","8" :  "farmer","9" :  "homemaker","10":  "K-12 student","11":  "lawyer","12":  "programmer","13":  "retired","14":  "sales/marketing","15":  "scientist","16":  "self-employed","17":  "technician/engineer","18":  "tradesman/craftsman","19":  "unemployed","20":  "writer"}



   gender = {"M" : 'male',"F" : 'female'}

   # users_df2 = users_df.rdd.map(lambda x: (x[0],gender[x[1]],x[2],x[3],x[4])).toDF(users_schema)

   # users_df2.show(2,truncate=False)


   broadcastGender = spark.sparkContext.broadcast(gender)

   def gender_convert(code):
      return broadcastGender.value[code]

   users_df2 = users_df.rdd.map(lambda x: (x[0],gender_convert(x[1]),x[2],Occupation[x[3]],x[4])).toDF(users_schema)

   # users_df2.show(2,truncate=False)
   # users_df2.printSchema()

	# *  1:  "Under 18"
	# * 18:  "18-24"
	# * 25:  "25-34"
	# * 35:  "35-44"
	# * 45:  "45-49"
	# * 50:  "50-55"
	# * 56:  "56+"

   users_df2 = users_df2.withColumn("age_range", when(col('Age') == 1,"Under 18")
                                 .when(col('Age') == 18,"18-24")
                                 .when(col('Age') == 25 ,"25-34")
                                 .when(col('Age') == 35,"35-44")
                                 .when(col('Age') == 45 ,"45-49")
                                 .when(col('Age') == 50,"50-55")
                                 .when(col('Age') == 56 ,"56+")
                                 .otherwise(col('Age')))


   # users_df2.show(2,truncate=False)

   users_df2.groupBy('gender').agg(count('*')).show()

# +------+--------+                                                               
# |gender|count(1)|
# +------+--------+
# |female|    1709|
# |  male|    4331|
# +------+--------+





   # df.createOrReplaceTempView("movies_data")
   # spark.sql("select Movie_genre  from movies_data") \
   #  .show(truncate= False)


   # spark.sql("select SPLIT(Movie_genre,'|') as NameArray from movies_data") \
   #  .show()



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

   
    
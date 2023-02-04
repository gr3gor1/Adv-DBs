from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import time
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("Q4").getOrCreate()

#create dataframe of yellow taxi trips
trips=spark.read.parquet("hdfs://192.168.0.1:9000/data/parq")

#create columns of year month day and hour of day
trips = trips.withColumn("year", date_format(to_date("tpep_pickup_datetime", "yyyy-MM-dd"), "yyyy"))\
	.withColumn("month", date_format(to_date("tpep_pickup_datetime", "yyyy-MM-dd"), "MMMM"))\
	.withColumn("day",date_format(to_date("tpep_pickup_datetime", "yyyy-MM-dd"), "EEEE"))\
	.withColumn("hour", date_format(col("tpep_pickup_datetime"), "k"))
#keep only records regarding the period January to June 2022
trips = trips.filter(trips["month"]!="July").filter(trips["year"]=="2022")
#find the total passengers in taxis in each day and hour
trips = trips.groupBy("day","hour").sum("Passenger_count").orderBy(unix_timestamp(col("hour"),"k"))
#create a window partioning by day and getting the sum of passenger count in descending order
windowDept = Window.partitionBy("day").orderBy(col("sum(Passenger_count)").desc())
#create an index over the previous window
trips = trips.withColumn("order_per_hour",row_number().over(windowDept))
#keep the 3 top hours per day
trips = trips.filter(col("order_per_hour") <= 3).drop("order_per_hour").collect()

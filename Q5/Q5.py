from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import time
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.master("spark://192.168.0.1:7077").appName("Q5").getOrCreate()

#create dataframe of yellow taxi trips
trips=spark.read.parquet("hdfs://192.168.0.1:9000/data/parq")


#create year month day day_of_month and tip percentage per trip columns
trips = trips.withColumn("year", date_format(to_date("tpep_pickup_datetime", "yyyy-MM-dd"), "yyyy"))\
	.withColumn("month", date_format(to_date("tpep_pickup_datetime", "yyyy-MM-dd"), "MMMM"))\
	.withColumn("day",date_format(to_date("tpep_pickup_datetime", "yyyy-MM-dd"), "EEEE"))\
	.withColumn("day_of_month", date_format(col("tpep_pickup_datetime"), "d"))\
	.withColumn("percentage",col("tip_amount")/col("fare_amount")*100)
#keep only records between January to June 2022
trips = trips.filter(trips["month"]!="July").filter(trips["year"]=="2022")
#group by month day of month and day and find the mean percentage
trips = trips.groupBy("month","day_of_month","day").mean("percentage")
#create a window partioning per month 
windowDept = Window.partitionBy("month").orderBy(col("avg(percentage)").desc())
#create an index column over the previous window
trips = trips.withColumn("order_per_month",row_number().over(windowDept))
#keep only the first five percentages and sort the months while preserving the ascending index for each month
trips = trips.filter(col("order_per_month") <= 5).orderBy(unix_timestamp(col("month"),"MMMM"),col("order_per_month").asc()).drop("order_per_month").collect()

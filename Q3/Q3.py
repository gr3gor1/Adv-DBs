from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import time
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.master("spark://192.168.0.1:7077").appName("Q3").getOrCreate()

#create dataframe of yellow taxi trips
trips=spark.read.parquet("hdfs://192.168.0.1:9000/data/parq")

#get month value year value and day of the year value based on the pickup timestamp and then keep only records where drop off location and pick up location are different
trips = trips.withColumn("year", date_format(to_date("tpep_pickup_datetime", "yyyy-MM-dd"), "yyyy"))\
    .withColumn("month", date_format(to_date("tpep_pickup_datetime", "yyyy-MM-dd"), "MMMM"))\
    .withColumn("day_of_the_year", date_format(to_date("tpep_pickup_datetime", "yyyy-MM-dd"), "D").cast("int"))\
    .filter(trips["PULocationID"]!=trips["DOLocationID"])
#keep only records in 2022 from January to June
trips = trips.filter(trips["year"]=="2022").filter(trips["month"]!="July")
#group by the 15 day period and find the mean values based on the day of the year column that we created earlier
trips = trips.groupBy(floor(col("day_of_the_year")/15).alias("15_day_period")).agg(mean("Trip_distance"),mean("Total_amount")).orderBy("15_day_period").collect()


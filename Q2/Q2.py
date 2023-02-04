from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import time
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("Q2").getOrCreate()

#create dataframe of yellow taxi trips parquet
trips=spark.read.parquet("hdfs://192.168.0.1:9000/data/parq")

#create year and month columns based on pickup timestamp
trips = trips.withColumn("year", date_format(to_date("tpep_pickup_datetime", "yyyy-MM-dd"), "yyyy")).withColumn("month", date_format(to_date("tpep_pickup_datetime", "yyyy-MM-dd"), "MMMM"))
#keep only records regarding January to June 2022 where the tolls amount aren't 0
trips = trips.filter(trips["tolls_amount"]!="0").filter(trips["month"]!="July").filter(trips["year"]=="2022")
#group by month finding the maximum tolls amount for each month
a = trips.groupBy("month").max("tolls_amount").withColumnRenamed("month","months")
#find records that comply with the values of variable "a"
trips = trips.join(a).filter((trips["month"] == a["months"]) & (trips["tolls_amount"] == a["max(tolls_amount)"])).orderBy(unix_timestamp(col("month"),"MMMM")).collect()


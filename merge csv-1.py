from pyspark.sql import SparkSession

from pyspark.sql import Row

from pyspark.sql.functions import monotonically_increasing_id
spark = SparkSession.builder.appName("csv-merge-rizwan").config("spark.dynamicAllocation.enabled", "true").config("spark.shuffle.service.enabled", "true").config("spark.executor.cores","5").config("spark.executor.memory", "36G").config("spark.driver.memory", "86G").config('spark.dynamicAllocation.maxExecutors','30').enableHiveSupport().getOrCreate()

import os
import calendar
import time
import string


sc = spark.sparkContext 
path = input("Enter Path: ")
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
list_status = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(path))
result = [file.getPath().getName() for file in list_status]
gzList = [ fi for fi in result if fi.endswith(".gz") ]
csvList = [ fi for fi in result if fi.endswith(".csv") ]


column_names = "ColA|ColB|ColC" 
temp = spark.createDataFrame( 
[ tuple('' for i in column_names.split("|")) 
], 
column_names.split("|") 
).where("1=0")


if (len(csvList) != 0):
    for i in range(len(csvList)):
        df = spark.read.csv(path + csvList[i], sep=',', header='true', inferSchema='true')
        df = df.withColumn("id", monotonically_increasing_id())
        temp = temp.withColumn("id", monotonically_increasing_id())
        temp = temp.join(df, "id", "outer").drop("id")
temp.show()
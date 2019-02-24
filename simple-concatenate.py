spark.stop()
sc.stop()

from pyspark.sql import SparkSession

from pyspark.sql import Row


spark = SparkSession.builder.appName("concat test-rizwan").config("spark.dynamicAllocation.enabled", "true").config("spark.shuffle.service.enabled", "true").config("spark.executor.cores","5").config("spark.executor.memory", "36G").config("spark.driver.memory", "86G").config('spark.dynamicAllocation.maxExecutors','30') .enableHiveSupport().getOrCreate()

import os
import calendar
import time
import string


sc = spark.sparkContext

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.read.parquet("path")

df.registerTempTable("df")

df1 = sqlContext.sql("SELECT CONCAT(TRIM(COMPANY_CODE),'-', TRIM(COUNTRY)) as concatenated_val FROM df")

df1.show()
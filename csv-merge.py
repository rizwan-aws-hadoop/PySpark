from pyspark.sql import SparkSession

from pyspark.sql import Row

from pyspark.sql.functions import monotonically_increasing_id
spark = SparkSession.builder.appName("csv-2-parquet-rizwan").config("spark.dynamicAllocation.enabled", "true").config("spark.shuffle.service.enabled", "true").config("spark.executor.cores","5").config("spark.executor.memory", "36G").config("spark.driver.memory", "86G").config('spark.dynamicAllocation.maxExecutors','30').enableHiveSupport().getOrCreate()

import os
import calendar
import time
import string

import os
from sys import stderr
import sys
import re
import subprocess

from subprocess import PIPE
sc = spark.sparkContext 
path = input("Enter Source Path: ")
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
list_status = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(path))
result = [file.getPath().getName() for file in list_status]
gzList = [ fi for fi in result if fi.endswith(".gz") ]
csvList = [ fi for fi in result if fi.endswith(".csv") ]

if len(csvList) != 0:
   src_path = path
   dest_path = input("Enter the Local Destination Path: ")
   filename = input("Enter the filename: ")
   command="""hadoop fs -getmerge %s/*.csv %s/%s""" % (src_path,dest_path, filename )
   pobj = subprocess.Popen(command, stdout=PIPE, stderr=PIPE, shell=True)

dest_hdfs_path = input("Enter the HDFS destination: ")
command = """hadoop fs -copyFromLocal %s/%s %s""" %(dest_path, filename, dest_hdfs_path)
pobj = subprocess.Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
stdo, stde = pobj.communicate()
exit_code = pobj.returncode
print(stde)

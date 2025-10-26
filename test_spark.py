import os
os.environ['JAVA_HOME'] = '/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home'

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()
print("Success! Spark version:", spark.version)
spark.stop()

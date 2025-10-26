import os
os.environ['JAVA_HOME'] = '/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home'

# Set additional Spark environment variables
os.environ['SPARK_HOME'] = os.path.join(os.path.dirname(__import__('pyspark').__file__))
os.environ['PYSPARK_PYTHON'] = 'python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python3'

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Try creating SparkContext first with explicit configuration
conf = SparkConf()
conf.setAppName("Test")
conf.setMaster("local[*]")

try:
    # Stop any existing context
    SparkContext.getOrCreate().stop()
except:
    pass

# Create new context
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

print("Success! Spark version:", spark.version)
print("SparkContext:", sc)
print("SparkSession:", spark)

spark.stop()

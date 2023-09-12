import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("Utility1FeederData").getOrCreate()

df = spark.read.csv("/Users/kateweber/dev/ESource-Case-Study/data/utility1_circuits/utility1_circuits.csv")


print(df.head())
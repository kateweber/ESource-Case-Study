import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType, DateType

spark = SparkSession.builder.appName("IEDRDataProcessor").getOrCreate()

def getSession():
    return spark
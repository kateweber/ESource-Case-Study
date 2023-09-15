import sys
sys.path.append('/Users/kateweber/dev/ESource-Case-Study/processors')

from sparkinit import *

def loadSource():
    spark = getSession()

    schema = StructType([
        StructField("Master_CDF", StringType(), nullable=False)
        , StructField("feeder_voltage", DecimalType(5,2), nullable=False)
        , StructField("feeder_max_hc", DecimalType(5,2), nullable=False)
        , StructField("feeder_min_hc", DecimalType(5,2), nullable=False)
        , StructField("feeder_dg_connected_since_refresh", DecimalType(5,2), nullable=False)
        , StructField("hca_refresh_date", StringType(), nullable=False)
        , StructField("color", StringType(), nullable=False)
        , StructField("shape_length", DecimalType(38,9), nullable=False)
    ])

    #todo: update to select all files in the utility1_circuits folder, should all be appended in the same df
    file_path = "/Users/kateweber/dev/ESource-Case-Study/source/utility2_circuits/utility2_circuits.csv"

    transformSQL = """
            SELECT *
            FROM utility2_feeder_raw
        """

    #todo: wrap in try/except 
    #todo: fix null handling
    df = spark.read \
        .options(header='True', delimiter=',', nullValue=None) \
        .schema(schema) \
        .csv(file_path)

    #todo: write to s3

    df.createOrReplaceTempView("utility2_feeder_raw")

    result = spark.sql(transformSQL)

    result.write.save('/Users/kateweber/dev/ESource-Case-Study/results/utility2_feeder_bronze', format='parquet', mode='overwrite')
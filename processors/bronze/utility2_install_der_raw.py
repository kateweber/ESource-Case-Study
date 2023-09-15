import sys
sys.path.append('/Users/kateweber/dev/ESource-Case-Study/processors')

from sparkinit import *

def loadSource():
    spark = getSession()

    schema = StructType([
        StructField("DER_ID", IntegerType(), nullable=False)
        , StructField("SERVICE_STREET_ADDRESS", StringType(), nullable=True)
        , StructField("DER_TYPE", StringType(), nullable=False)
        , StructField("DER_NAMEPLATE_RATING", DecimalType(10,4), nullable=False)
        , StructField("DER_INTERCONNECTION_LOCATION", StringType(), nullable=False)
        , StructField("INTERCONNECTION_COST", DecimalType(38,2), nullable=False)
    ])

    #todo: update to select all files in the utility1_circuits folder, should all be appended in the same df
    file_path = "/Users/kateweber/dev/ESource-Case-Study/source/utility2_install_der/utility2_install_der.csv"

    transformSQL = """
            SELECT *
            FROM utility2_install_der_raw
        """

    #todo: wrap in try/except 
    #todo: fix null handling
    df = spark.read \
        .options(header='True', delimiter=',', nullValue=None) \
        .schema(schema) \
        .csv(file_path)

    #todo: write to s3

    df.createOrReplaceTempView("utility2_install_der_raw")

    result = spark.sql(transformSQL)

    result.write.save('/Users/kateweber/dev/ESource-Case-Study/results/utility2_install_der_bronze', format='parquet', mode='overwrite')
import sys
sys.path.append('/Users/kateweber/dev/ESource-Case-Study/processors')

from sparkinit import *

def loadSource():
    spark = getSession()

    schema = StructType([
        StructField("DER_TYPE", StringType(), nullable=False)
        , StructField("DER_NAMEPLATE_RATING", DecimalType(10,4), nullable=False)
        , StructField("INVERTER_NAMEPLATE_RATING", DecimalType(10,4), nullable=True)
        , StructField("PLANNED_INSTALLATION_DATE", DateType(), nullable=True)
        , StructField("DER_STATUS", StringType(), nullable=True)
        , StructField("DER_STATUS_RATIONALE", StringType(), nullable=True)
        , StructField("TOTAL_MW_FOR_SUBSTATION", DecimalType(10,4), nullable=True)
        , StructField("INTERCONNECTION_QUEUE_REQUEST_ID", IntegerType(), nullable=True)
        , StructField("INTERCONNECTION_QUEUE_POSITION", TimestampType(), nullable=True)
        , StructField("DER_INTERCONNECTION_LOCATION", StringType(), nullable=True)
    ])

    #todo: update to select all files in the utility1_circuits folder, should all be appended in the same df
    file_path = "/Users/kateweber/dev/ESource-Case-Study/source/utility2_planned_der/utility2_planned_der.csv"

    transformSQL = """
            SELECT *
            FROM utility2_planned_der_raw
        """

    #todo: wrap in try/except 
    #todo: fix null handling
    df = spark.read \
        .options(header='True', delimiter=',', nullValue=None) \
        .schema(schema) \
        .csv(file_path)

    #todo: write to s3

    df.createOrReplaceTempView("utility2_planned_der_raw")

    result = spark.sql(transformSQL)

    result.write.save('/Users/kateweber/dev/ESource-Case-Study/results/utility2_planned_der_bronze', format='parquet', mode='overwrite')
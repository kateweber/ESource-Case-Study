import sys
sys.path.append('/Users/kateweber/dev/ESource-Case-Study/processors')

from sparkinit import *

def loadSource():
    spark = getSession()

    schema = StructType([
        StructField("", IntegerType(), nullable=True)
        , StructField("Circuits_Phase3_CIRCUIT", IntegerType(), nullable=False)
        , StructField("Circuits_Phase3_NUMPHASES", IntegerType(), nullable=False)
        , StructField("Circuits_Phase3_OVERUNDER", StringType(), nullable=False)
        , StructField("Circuits_Phase3_PHASE", StringType(), nullable=True)
        , StructField("NYHCPV_csv_NSECTION", IntegerType(), nullable=False)
        , StructField("NYHCPV_csv_NFEEDER", IntegerType(), nullable=False)
        , StructField("NYHCPV_csv_NVOLTAGE", DecimalType(5,2), nullable=False)
        , StructField("NYHCPV_csv_NMAXHC", DecimalType(5,2), nullable=False)
        , StructField("NYHCPV_csv_NMAPCOLOR", StringType(), nullable=False)
        , StructField("NYHCPV_csv_FFEEDER", IntegerType(), nullable=False)
        , StructField("NYHCPV_csv_FVOLTAGE", DecimalType(5,2), nullable=False)
        , StructField("NYHCPV_csv_FMAXHC", DecimalType(5,2), nullable=False)
        , StructField("NYHCPV_csv_FMINHC", DecimalType(5,2), nullable=False)
        , StructField("NYHCPV_csv_FHCADATE", TimestampType(), nullable=False)
        , StructField("NYHCPV_csv_FNOTES", StringType(), nullable=True)
        , StructField("Shape_Length", DecimalType(38,9), nullable=False)
    ])

    #todo: update to select all files in the utility1_circuits folder, should all be appended in the same df
    file_path = "/Users/kateweber/dev/ESource-Case-Study/source/utility1_circuits/utility1_circuits.csv"

    transformSQL = """
            SELECT *
            FROM utility1_feeder_raw
        """

    #todo: wrap in try/except 
    #todo: fix null handling
    df = spark.read \
        .options(header='True', delimiter=',', nullValue=None) \
        .schema(schema) \
        .csv(file_path)

    df1 = df.drop('')

    #todo: write to s3

    df1.createOrReplaceTempView("utility1_feeder_raw")

    result = spark.sql(transformSQL)

    result.write.save('/Users/kateweber/dev/ESource-Case-Study/results/utility1_feeder_bronze', format='parquet', mode='overwrite')
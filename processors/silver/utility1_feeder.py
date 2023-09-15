import sys
sys.path.append('/Users/kateweber/dev/ESource-Case-Study/processors')

from sparkinit import *

def cleanData():
    spark = getSession()

    file_path = "/Users/kateweber/dev/ESource-Case-Study/results/utility1_feeder_bronze"

    transformSQL = """
            SELECT DISTINCT
                Circuits_Phase3_CIRCUIT as circuit_id
                , NYHCPV_csv_FVOLTAGE as feeder_voltage
                , NYHCPV_csv_FMAXHC as feeder_max_hc
                , NYHCPV_csv_FMINHC as feeder_min_hc
                , NYHCPV_csv_NMAPCOLOR as map_color
                , to_date(substr(NYHCPV_csv_FHCADATE, 0, 10), 'yyyy-MM-dd') as hca_refresh_date
                , Shape_Length as shape_length
            FROM utility1_feeder
        """

    #todo: wrap in try/except 
    #todo: fix null handling
    df = spark.read.load(file_path)

    df1 = df.drop('')

    #todo: write to s3

    df1.createOrReplaceTempView("utility1_feeder")

    result = spark.sql(transformSQL)

    result.write.save('/Users/kateweber/dev/ESource-Case-Study/results/utility1_feeder_silver', format='parquet', mode='overwrite')
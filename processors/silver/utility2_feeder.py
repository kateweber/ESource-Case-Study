import sys
sys.path.append('/Users/kateweber/dev/ESource-Case-Study/processors')

from sparkinit import *

def cleanData():
    spark = getSession()

    file_path = "/Users/kateweber/dev/ESource-Case-Study/results/utility2_feeder_bronze"

    transformSQL = """
            SELECT 
                Master_CDF as circuit_id
                , feeder_voltage
                , feeder_max_hc
                , feeder_min_hc
                , color as map_color
                , to_date(substr(hca_refresh_date, 0, 10), 'yyyy/MM/dd') as hca_refresh_date
                , shape_length
            FROM utility2_feeder
        """

    df = spark.read.load(file_path)

    #todo: write to s3

    df.createOrReplaceTempView("utility2_feeder")

    result = spark.sql(transformSQL)

    result.write.save('/Users/kateweber/dev/ESource-Case-Study/results/utility2_feeder_silver', format='parquet', mode='overwrite')
import sys
sys.path.append('/Users/kateweber/dev/ESource-Case-Study/processors')

from sparkinit import *

def cleanData():
    spark = getSession()

    file_path = "/Users/kateweber/dev/ESource-Case-Study/results/utility2_planned_der_bronze"

    transformSQL = """
            SELECT 
                string(null) as der_id
                , DER_TYPE as der_type
                , DER_NAMEPLATE_RATING as namplate_rating
                , DER_INTERCONNECTION_LOCATION as circuit_id
                , 'Planned' as status
                , PLANNED_INSTALLATION_DATE as planned_install_date
            FROM utility2_planned_der
        """

    #todo: wrap in try/except 
    #todo: fix null handling
    df = spark.read.load(file_path)

    #todo: write to s3

    df.createOrReplaceTempView("utility2_planned_der")

    result = spark.sql(transformSQL)

    result.write.save('/Users/kateweber/dev/ESource-Case-Study/results/utility2_planned_der_silver', format='parquet', mode='overwrite')
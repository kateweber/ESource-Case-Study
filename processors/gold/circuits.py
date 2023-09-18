import sys
sys.path.append('/Users/kateweber/dev/ESource-Case-Study/processors')

from sparkinit import *

def finalTransform():
    spark = getSession()

    file_path_utility1 = "/Users/kateweber/dev/ESource-Case-Study/results/utility1_feeder_silver"
    file_path_utility2 = "/Users/kateweber/dev/ESource-Case-Study/results/utility2_feeder_silver"

    transformSQL = """
            SELECT *
            FROM utility1_feeder

            UNION ALL

            SELECT *
            FROM utility2_feeder
        """

    #todo: wrap in try/except 
    df_util1 = spark.read.load(file_path_utility1)
    df_util2 = spark.read.load(file_path_utility2)

    #todo: write to s3

    df_util1.createOrReplaceTempView("utility1_feeder")
    df_util2.createOrReplaceTempView("utility2_feeder")

    result = spark.sql(transformSQL)

    result.write.save('/Users/kateweber/dev/ESource-Case-Study/results/circuits_gold', format='parquet', mode='overwrite')
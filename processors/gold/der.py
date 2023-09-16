import sys
sys.path.append('/Users/kateweber/dev/ESource-Case-Study/processors')

from sparkinit import *

def finalTransform():
    spark = getSession()

    file_path_utility1_install = "/Users/kateweber/dev/ESource-Case-Study/results/utility1_install_der_silver"
    file_path_utility1_planned = "/Users/kateweber/dev/ESource-Case-Study/results/utility1_planned_der_silver"
    file_path_utility2_install = "/Users/kateweber/dev/ESource-Case-Study/results/utility2_install_der_silver"
    file_path_utility2_planned = "/Users/kateweber/dev/ESource-Case-Study/results/utility2_planned_der_silver"

    transformSQL = """
            SELECT *
            FROM utility1_install

            UNION

            SELECT *
            FROM utility1_planned

            UNION

            SELECT *
            FROM utility2_install

            UNION

            SELECT *
            FROM utility2_planned
        """

    #todo: wrap in try/except 
    #todo: fix null handling
    df_util1_i = spark.read.load(file_path_utility1_install)
    df_util1_p = spark.read.load(file_path_utility1_planned)
    df_util2_i = spark.read.load(file_path_utility2_install)
    df_util2_p = spark.read.load(file_path_utility2_planned)

    #todo: write to s3

    df_util1_i.createOrReplaceTempView("utility1_install")
    df_util1_p.createOrReplaceTempView("utility1_planned")
    df_util2_i.createOrReplaceTempView("utility2_install")
    df_util2_p.createOrReplaceTempView("utility2_planned")

    result = spark.sql(transformSQL)

    result.write.save('/Users/kateweber/dev/ESource-Case-Study/results/der_gold', format='parquet', mode='overwrite')

    result.show()
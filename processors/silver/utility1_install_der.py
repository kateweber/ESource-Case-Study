import sys
sys.path.append('/Users/kateweber/dev/ESource-Case-Study/processors')

from sparkinit import *

def cleanData():
    spark = getSession()

    file_path = "/Users/kateweber/dev/ESource-Case-Study/results/utility1_install_der_bronze"

    transformSQL = """
            with unpivoted as (
                SELECT *
                FROM utility1_install_der
                UNPIVOT (
                    val FOR der_type IN (SolarPV, EnergyStorageSystem, Wind, MicroTurbine, SynchronousGenerator, InductionGenerator, FarmWaste, FuelCell, CombinedHeatandPower, GasTurbine, Hydro, InternalCombustionEngine, SteamTurbine, Other)
                )
            )
            SELECT 
                ProjectID as der_id
                , der_type
                , NamePlateRating as namplate_rating
                , ProjectCircuitID as circuit_id
                , 'Installed' as status
                , to_date(null) as planned_install_date
            FROM unpivoted 
            WHERE val > 0
        """

    #todo: wrap in try/except 
    df = spark.read.load(file_path)

    #todo: write to s3

    df.createOrReplaceTempView("utility1_install_der")

    result = spark.sql(transformSQL)

    result.write.save('/Users/kateweber/dev/ESource-Case-Study/results/utility1_install_der_silver', format='parquet', mode='overwrite')
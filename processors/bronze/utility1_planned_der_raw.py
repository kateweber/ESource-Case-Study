import sys
sys.path.append('/Users/kateweber/dev/ESource-Case-Study/processors')

from sparkinit import *

def loadSource():
    spark = getSession()

    schema = StructType([
        StructField("ProjectType", StringType(), nullable=False)
        , StructField("NamePlateRating", DecimalType(38,2), nullable=False)
        , StructField("InServiceDate", DateType(), nullable=True)
        , StructField("ProjectStatus", StringType(), nullable=True)
        , StructField("ProjectID", IntegerType(), nullable=False)
        , StructField("CompletionDate", StringType(), nullable=True)
        , StructField("ProjectCircuitID", StringType(), nullable=False)
        , StructField("Hybrid", StringType(), nullable=False)
        , StructField("SolarPV", DecimalType(5,4), nullable=False)
        , StructField("EnergyStorageSystem", IntegerType(), nullable=False)
        , StructField("Wind", IntegerType(), nullable=False)
        , StructField("MicroTurbine", IntegerType(), nullable=False)
        , StructField("SynchronousGenerator", IntegerType(), nullable=False)
        , StructField("InductionGenerator", IntegerType(), nullable=False)
        , StructField("FarmWaste", IntegerType(), nullable=False)
        , StructField("FuelCell", IntegerType(), nullable=False)
        , StructField("CombinedHeatandPower", IntegerType(), nullable=False)
        , StructField("GasTurbine", IntegerType(), nullable=False)
        , StructField("Hydro", IntegerType(), nullable=False)
        , StructField("InternalCombustionEngine", IntegerType(), nullable=False)
        , StructField("SteamTurbine", IntegerType(), nullable=False)
        , StructField("Other", IntegerType(), nullable=False)
    ])

    #todo: update to select all files in the utility1_circuits folder, should all be appended in the same df
    file_path = "/Users/kateweber/dev/ESource-Case-Study/source/utility1_planned_der/utility1_planned_der.csv"

    transformSQL = """
            SELECT *
            FROM utility1_planned_der_raw
        """

    #todo: wrap in try/except 
    #todo: fix null handling
    df = spark.read \
        .options(header='True', delimiter=',', nullValue=None) \
        .schema(schema) \
        .csv(file_path)

    #todo: write to s3

    df.createOrReplaceTempView("utility1_planned_der_raw")

    result = spark.sql(transformSQL)

    result.write.save('/Users/kateweber/dev/ESource-Case-Study/results/utility1_planned_der_bronze', format='parquet', mode='overwrite')
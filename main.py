from fastapi import FastAPI
app = FastAPI()

import sys
sys.path.append('/Users/kateweber/dev/ESource-Case-Study/processors/bronze')
sys.path.append('/Users/kateweber/dev/ESource-Case-Study/processors/silver')
sys.path.append('/Users/kateweber/dev/ESource-Case-Study/processors/gold')
sys.path.append('/Users/kateweber/dev/ESource-Case-Study/processors')

import sparkinit

#Bronze processors
import utility1_feeder_raw, utility2_feeder_raw, utility1_install_der_raw, utility1_planned_der_raw, utility2_install_der_raw, utility2_planned_der_raw

#Silver processors
import utility1_feeder, utility2_feeder, utility1_install_der, utility1_planned_der, utility2_install_der, utility2_planned_der

#Gold processors
import circuits, der

@app.get("/refreshData")
def refreshData():

    #todo: wrap in try except and wait for return
    utility1_feeder_raw.loadSource()
    utility2_feeder_raw.loadSource()
    utility1_install_der_raw.loadSource()
    utility2_install_der_raw.loadSource()
    utility1_planned_der_raw.loadSource()
    utility2_planned_der_raw.loadSource()

    #todo: call silver level refresh jobs if bronze succeeded
    utility1_feeder.cleanData()
    utility2_feeder.cleanData()
    utility1_install_der.cleanData()
    utility1_planned_der.cleanData()
    utility2_install_der.cleanData()
    utility2_planned_der.cleanData()

    #todo: call gold level refresh jobs if silver succeeded
    circuits.finalTransform()
    der.finalTransform()

    return

@app.get("/getFeeders")
def getFeeders(maxHostingCapacity):
    spark = sparkinit.getSession()

    file_path = "/Users/kateweber/dev/ESource-Case-Study/results/circuits_gold"

    transformSQL = """
            SELECT *
            FROM circuits
            WHERE feeder_max_hc > {0}
        """.format(maxHostingCapacity)

    #todo: wrap in try/except 
    df = spark.read.load(file_path)
    #todo: write to s3
    df.createOrReplaceTempView("circuits")
    result = spark.sql(transformSQL)
    return result.toJSON().collect()

@app.get("/getDERsForFeeder")
def getAllDers(feederID):
    spark = sparkinit.getSession()

    file_path = "/Users/kateweber/dev/ESource-Case-Study/results/der_gold"

    transformSQL = """
            SELECT *
            FROM der
            WHERE circuit_id = "{0}"
            ORDER BY der_id
        """.format(feederID)

    #todo: wrap in try/except 
    df = spark.read.load(file_path)
    #todo: write to s3
    df.createOrReplaceTempView("der")
    result = spark.sql(transformSQL)
    return result.toJSON().collect()
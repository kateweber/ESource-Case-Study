import sys
sys.path.append('/Users/kateweber/dev/ESource-Case-Study/processors/bronze')
sys.path.append('/Users/kateweber/dev/ESource-Case-Study/processors/silver')
sys.path.append('/Users/kateweber/dev/ESource-Case-Study/processors/gold')

#Bronze processors
import utility1_feeder_raw, utility2_feeder_raw, utility1_install_der_raw, utility1_planned_der_raw, utility2_install_der_raw, utility2_planned_der_raw

#Silver processors
import utility1_feeder, utility2_feeder

#Gold processors
import circuits

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

    #todo: call gold level refresh jobs if silver succeeded
    circuits.finalTransform()

    return

def getFeeders(maxHostingCapacity):
    result = ''
    #todo: get table from gold layer, select * where max capacity > maxHostingCapacity

    return result

def getAllDers(feederID):
    result = ''
    #todo: get table from gold layer, select * where circuit_id = feederId

    return result

#todo: implement monthly scheduler or event or check for new files (can we put CRON in here?)
refreshData()
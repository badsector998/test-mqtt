import yaml
import json
from json import JSONEncoder
import datetime
from pyrfc3339 import generator

class DateTimeEncoder(JSONEncoder):
        #Override the default method
        def default(self, obj):
            if isinstance(obj, (datetime.date, datetime.datetime)):
                return generator.generate(obj)

def parseConfig(file):
    with open(file, 'r') as stream:
        try:
            parsed = yaml.safe_load(stream)
            return parsed
        except yaml.YAMLError as e:
            return e

def loadConf(file):
    global_conf = parseConfig(file)
    db_conf = global_conf['database']
    apis_conf = global_conf['apis']
    return db_conf, apis_conf

def parseData(data):
    stack_mqtt_id = data[0]
    current_parameter_name = data[1]
    current_hq_parameter_name = data[2]
    value = data[3]
    corrected_value = data[4]
    measured_at = data[5]
    stack_condition = data[6]

    if current_hq_parameter_name != "":
        current_parameter_name = current_hq_parameter_name

    payload_build = {
                     'name':current_parameter_name, 
                     'value':value, 
                     'value_condition_override':corrected_value, 
                     'timestamp':measured_at, 
                     'condition':stack_condition
                    }

    return stack_mqtt_id, payload_build


def createPayload(key, value):
    build_payload = {'cems' : key, 'payload' : [value]}
    payload = json.dumps(build_payload, indent=4, cls=DateTimeEncoder)
    return payload
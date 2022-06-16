from copy import deepcopy
import json
from json import JSONEncoder
from turtle import update
from numpy import append
import psycopg2
import datetime

pd1_cems2 = " 849c2bf6-6b33-4400-b45f-2f0ba4dacec5"
pd1_boiler = "600154a3-8a1e-41d6-8aab-05f31174a7bb"

class DateTimeEncoder(JSONEncoder):
        #Override the default method
        def default(self, obj):
            if isinstance(obj, (datetime.date, datetime.datetime)):
                return obj.isoformat()

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
                     "name":current_parameter_name, 
                     "value":value, 
                     "value_condition_override":corrected_value, 
                     "timestamp":measured_at, 
                     "condition":stack_condition
                    }

    return stack_mqtt_id, payload_build

#connect to local postgre
connection = psycopg2.connect(database="postgres",
                              host="localhost",
                              user="postgres",
                              password="example-password",
                              port="5432")

cursor = connection.cursor()

#create query
cursor.execute("select stack_mqtt_id, current_parameter_name, current_hq_parameter_name, value, corrected_value, measured_at, stack_condition from measurements order by measured_at desc limit 50")

#save query result
dummy_data = cursor.fetchall()

measurement = dict()

#fetch, parse, group data
for i in range(len(dummy_data)):
    #parse data
    stack_mqtt_id, payload = parseData(dummy_data[i])
    # print(payload)

    #check if key exist, if not create new dict
    if stack_mqtt_id not in measurement:
        measurement[stack_mqtt_id] = []

    #assign list of list values to dict
    measurement[stack_mqtt_id] = append(measurement[stack_mqtt_id], payload).tolist()

#create payload
for key in measurement:
    # print(key + " : ")
    # print(measurement[key])
    # print(convertPayload(measurement[key]))
    create_payload = {"cems " : key, "payload " : measurement[key]}
    payload = json.dumps(create_payload, indent=4, cls=DateTimeEncoder)
    print(payload)

# todo : create mqtt client for publishing the payload




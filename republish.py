import json
from json import JSONEncoder
import psycopg2
import datetime
import paho.mqtt.client as mqtt
from pyrfc3339 import generator
import yaml

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

def openQuery(file):
    with open(file, 'r') as stream:
        query = stream.read().replace('\n', ' ')
    return query

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


#callback function
def on_connect(client, userdata, flags, rc):
    print("connected with status : " + str(rc))
    client.subscribe(topic)
    # client.publish(topic, payload)

#callback function
def on_publish(client, userdata, result):
    print("Data published")
    
def on_message(client, userdata,msg):
    print(msg.topic)
    print(msg.payload)
    payload = json.loads(msg.payload)
    print(payload)
    client.disconnect()

#============== main =================

#config.yaml parse
global_conf = parseConfig("config.yaml")
db_conf = global_conf['database']
apis_conf = global_conf['apis']

#connect to local postgre
connection = psycopg2.connect(database=db_conf['db'],
                              host=db_conf['host'],
                              user=db_conf['user'],
                              password=db_conf['password'],
                              port=db_conf['port'])
cursor = connection.cursor()

#load query from text file
query_string = openQuery("query.txt")

#create query
cursor.execute(query_string)

#save query result
data = cursor.fetchall()

#mqtt info
broker = apis_conf['broker']
topic = apis_conf['topic']
port = apis_conf['port']

#create mqtt client
client = mqtt.Client()

#call on publish callback function
client.on_publish = on_publish
#call connect callback function
client.on_connect = on_connect
#call message callback function
client.on_message = on_message

#connect to broker target
client.connect(broker, port, 60)

#fetch, parse, send
for ms in data:
    #parse data
    stack_mqtt_id, payload = parseData(ms)
    create_payload = {'cems' : stack_mqtt_id, 'payload' : [payload]}
    payload = json.dumps(create_payload, indent=4, cls=DateTimeEncoder)
    print(payload)
    index = data.index(ms)
    print("Data : " + str(index))
    client.publish(topic, payload)





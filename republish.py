import json
from json import JSONEncoder
import psycopg2
import datetime
import paho.mqtt.client as mqtt
from pyrfc3339 import generator

class DateTimeEncoder(JSONEncoder):
        #Override the default method
        def default(self, obj):
            if isinstance(obj, (datetime.date, datetime.datetime)):
                return generator.generate(obj)

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


# todo : create yaml parser. get database info.
#        store it to connection properties.

#connect to local postgre
connection = psycopg2.connect(database="postgres",
                              host="localhost",
                              user="postgres",
                              password="example-password",
                              port="5432")


cursor = connection.cursor()
#create query
cursor.execute("""
                select stack_mqtt_id, current_parameter_name, current_hq_parameter_name, value, corrected_value, measured_at, stack_condition 
                from measurements 
                where extract('month' from measured_at) = 6
                order by measured_at asc
                limit 50
                """)

#save query result
data = cursor.fetchall()
#create measurement dict for storing query result
measurement = dict()
#mqtt info
staging = "localhost"
topic = "test-topic"
port = 1883

#create mqtt client
client = mqtt.Client()

#call on publish callback function
client.on_publish = on_publish
#call connect callback function
client.on_connect = on_connect
#call message callback function
client.on_message = on_message


#connect to broker target
client.connect(staging, 1883, 60)


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





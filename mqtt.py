import random
import time
import paho.mqtt.client as mqtt
import json
import sched

#mqtt info
staging = ""
staging2 = ""
localhost = "localhost"
topic = ""
topic2 = "test-topic"
port = 1883
localport = 1883
pd1_cems2 = "849c2bf6-6b33-4400-b45f-2f0ba4dacec5"
pd1_boiler = "600154a3-8a1e-41d6-8aab-05f31174a7bb"
s = sched.scheduler(time.time, time.sleep)

#for now the payload is saved in corresponding file
#load json file
with open('pd1-boiler.json') as f:
    data = json.load(f)

#recreate json object to string json
datas = json.dumps(data, indent=4)
print(datas)

#callback function
def on_connect(client, userdata, flags, rc):
    print("connected with status : " + str(rc))
    client.subscribe(topic)
    client.publish(topic, datas)

#callback function
def on_publish(client, userdata, result):
    print("Data published")
    
def on_message(client, userdata,msg):
    print(msg.topic)
    print(msg.payload)
    payload = json.loads(msg.payload)
    print(payload)
    client.disconnect()

#create mqtt client
client = mqtt.Client()

#call publish callback function
client.on_publish = on_publish
#call connect callback function
client.on_connect = on_connect
#call message callback function
client.on_message = on_message

#connect to broker target
client.connect(staging, 1883, 60)

client.loop_forever()


# client.disconnect()

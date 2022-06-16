import random
import time
import paho.mqtt.client as mqtt
import json

#mqtt info
staging = "dashboard.cems.intusi.com"
localhost = "localhost"
topic = "c3m5-4pp"
port = 1883
localport = 1883
pd1_cems2 = "849c2bf6-6b33-4400-b45f-2f0ba4dacec5"
pd1_boiler = "600154a3-8a1e-41d6-8aab-05f31174a7bb"

#callback function
def on_connect(client, userdata, flags, rc):
    print("connected with status : " + str(rc))

#callback function
def on_publish(client, userdata, result):
    print("Data published")
    pass

#create mqtt client
client = mqtt.Client()
#connect client to broker target
client.connect(staging, port)
client.on_connect = on_connect
#call publish callback function
client.on_publish = on_publish

#for now the payload is saved in corresponding file
#load json file
with open('pd1-boiler.json') as f:
    data = json.load(f)

#recreate json object to string json
datas = json.dumps(data, indent=4)
print(datas)

client.publish(topic, datas)
# for i in range(20):
#     client.publish(topic, datas)
#     time.sleep(2)


# client.disconnect()

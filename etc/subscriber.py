from pydoc import cli
import paho.mqtt.client as mqtt
from psycopg2 import connect

# mqtt info
host = "localhost"
port = 1883
timelive = 60


# on connect callback function
def on_connect(client, userdata, flags, rc):
    print("connected with result code : " + str(rc))
    client.subscribe("-")


# on message callback function
def on_message(client, userdata, msg):
    print(msg.payload.decode())


# create client then connect
client = mqtt.Client("Test-Subscriber")
client.connect(host, port, timelive)

# call the on_connect callback function
client.on_connect = on_connect
# stand by for cacthing published messaged on the broker
client.on_message = on_message

client.loop_forever()

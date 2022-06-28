import json
import paho.mqtt.client as mqtt

class repoMqtt:
    def __init__(self, broker, topic, port, keepAlive) -> None:
        self.broker = broker
        self.topic = topic
        self.port = port
        self.keepAlive = keepAlive
        self.client = self.createMqttClient()

    def createMqttClient(self):
        #create mqtt client instance
        client = mqtt.Client()
        #call on publish callback function
        client.on_publish = on_publish
        #call connect callback function
        client.on_connect = on_connect
        #call message callback function
        client.on_message = on_message
        client.connect(self.broker, self.port, self.keepAlive)
        return client

    def sendPayload(self, payload):
        self.client.publish(self.topic, payload)

#callback function
def on_connect(self, client, userdata, flags, rc):
    print("connected with status : " + str(rc))
    # client.subscribe(self.topic)
    # client.publish(topic, payload)

#callback function
def on_publish(client, userdata, result):
    print("Data published")

#callback function       
def on_message(client, userdata,msg):
    print(msg.topic)
    print(msg.payload)
    payload = json.loads(msg.payload)
    print(payload)
    client.disconnect()
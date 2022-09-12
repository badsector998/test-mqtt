import json
import paho.mqtt.client as mqtt


class repoMqtt:
    def __init__(self, broker, topic, port, keepAlive) -> None:
        self.broker = broker
        self.topic = topic
        self.port = port
        self.keepAlive = keepAlive
        # self.client = self.createMqttClient()

    def createMqttClient(self):
        # create mqtt client instance
        client = mqtt.Client(client_id="mqtt-april")
        # call the callbacks function
        client.on_publish = on_publish
        client.on_message = on_message
        # connect
        # client.connect(self.broker, self.port, self.keepAlive)
        return client

    def sendPayload(self, payload):
        self.client.publish(self.topic, payload, qos=1)


def on_connect(client, userdata, flags, rc):
    print("connected with status : " + str(rc))


def on_publish(client, userdata, result):
    print("Publish call is done")


def on_message(client, userdata, msg):
    print(msg.topic)
    print(msg.payload)
    payload = json.loads(msg.payload)
    print(payload)
    client.disconnect()


def on_disconnect(client, userdata, rc):
    print("DisConnected result code " + str(rc))

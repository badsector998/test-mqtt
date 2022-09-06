import time
from numpy import append
from lib import repo
from lib.db import db_instance
from lib.mqtt import repoMqtt


def initiateProgram():
    db_conf, api_conf = repo.loadConf("config.yaml")
    broker = api_conf['broker']
    topic = api_conf['topic']
    port = api_conf['port']
    return db_conf, broker, topic, port


def run():
    db_conf, broker, topic, port = initiateProgram()
    cl = repoMqtt(broker, topic, port, 60)
    client = cl.createMqttClient()
    client.connect(cl.broker, cl.port, 60)
    client.loop_start()
    db = db_instance(db_conf)
    data = db.executeQuery("query.txt")
    measurement = {}
    for ms in data:
        stack_mqtt_id, values = repo.parseData(ms)
        if stack_mqtt_id not in measurement:
            measurement[stack_mqtt_id] = []
        measurement[stack_mqtt_id] = append(
                                                measurement[stack_mqtt_id],
                                                values
                                            ).tolist()

    for key in measurement:
        payload = repo.createPayload(key, measurement[key])
        print(payload)
        info = client.publish(cl.topic, payload)
        info.wait_for_publish(2)
        print(
                info.is_published(),
                cl.broker,
                cl.port,
                cl.topic,
                len(measurement[key])
            )
        time.sleep(10)

    client.loop_stop()
    client.disconnect()

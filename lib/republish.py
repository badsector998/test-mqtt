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
    repo.createDebugLog("Mqtt client connected")
    client.loop_start()
    repo.createDebugLog("Mqtt client starting loop")
    dbIns = db_instance(db_conf)
    repo.createDebugLog("DB instance has been created")
    data = dbIns.executeQuery("query.txt")
    repo.createDebugLog("Query has been executed")
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
        msgLength = len(measurement[key])
        repo.createDebugLog("Destination info : ")
        repo.createDebugLog(cl.broker + ", " + str(cl.port) + ", " + cl.topic)
        repo.createDebugLog("Message length : " + str(msgLength))
        repo.createDebugLog("sending payload : ")
        repo.createDebugLog(payload)
        info = client.publish(cl.topic, payload)
        repo.createDebugLog("Wait for publish")
        info.wait_for_publish(2)
        repo.createDebugLog("Published status : ")
        repo.createDebugLog(info.is_published())
        repo.createDebugLog("wait for another payload")
        time.sleep(10)

    repo.createDebugLog("all payload has been sent, closing mqtt connection")
    client.loop_stop()
    client.disconnect()
    repo.createDebugLog("Closing db connection")
    dbIns.conn.close()
    dbIns.csr.close()
    repo.createDebugLog("Closed db connection")
    del dbIns.conn
    del dbIns.csr

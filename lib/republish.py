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
    repo.Logs("Mqtt client connected")
    client.loop_start()
    repo.Logs("Mqtt client starting loop")
    dbIns = db_instance(db_conf)
    repo.Logs("DB instance has been created")
    data = dbIns.executeQuery("query.txt")
    repo.Logs("Query has been executed")
    measurement = {}
    for ms in data:
        stack_mqtt_id, values = repo.parseData(ms)
        if stack_mqtt_id not in measurement:
            measurement[stack_mqtt_id] = []
        measurement[stack_mqtt_id] = append(
                                                measurement[stack_mqtt_id],
                                                values
                                            ).tolist()

    for k, v in measurement.items():
        for i in range(len(v)):
            temp = []
            temp = append(temp, v).tolist()
            if i % 100 == 0:
                payload = repo.createPayload(k, temp)
                msgLength = len(temp)
                repo.Logs(f"Dest info : {cl.broker}, {cl.port}, {cl.topic}")
                repo.Logs("Message length : " + str(msgLength))
                repo.Logs(f"sending payload : {payload}")
                info = client.publish(cl.topic, payload)
                repo.Logs("Wait for publish")
                info.wait_for_publish(2)
                repo.Logs(f"Published status : {info.is_published()}")
                repo.Logs("wait for another payload")
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

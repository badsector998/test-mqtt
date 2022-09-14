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

    for k in measurement:
        temp = []
        for i in range(len(measurement[k])):
            index = i + 1
            repo.Logs(f"data {index}")
            temp = append(temp, measurement[k][i]).tolist()
            if index % 10000 == 0:
                repo.Logs(f"temp values : {temp}")
                payload = repo.createPayload(k, temp)
                msgLength = len(temp)
                repo.Logs(f"Dest info : {cl.broker}, {cl.port}, {cl.topic}")
                repo.Logs(f"sending payload for : {k}")
                repo.Logs("Message length : " + str(msgLength))
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

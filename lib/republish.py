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
    db = db_instance(db_conf)
    data = db.executeQuery("query.txt")
    for ms in data:
        stack_mqtt_id, values = repo.parseData(ms)
        payload = repo.createPayload(stack_mqtt_id, values)
        # print(payload)
        repo.createDebugLog(payload)
        cl = repoMqtt(broker, topic, port, 60)
        cl.sendPayload(payload)






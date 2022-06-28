from lib import repo
from lib import db
from lib.db import db_instance
from lib.mqtt import repoMqtt

def initiateProgram():
    db_conf, api_conf = repo.loadConf("config.yaml")
    broker = api_conf['broker']
    topic = api_conf['topic']
    port = api_conf['port']
    new_db = db_instance(db_conf)
    return new_db, broker, topic, port

def interactDB(db_conn, queryText):
    connection = db_conn.connect()
    cursor = connection.cursor()
    query_string = db.openQuery(queryText)
    cursor.execute(query_string)
    data = cursor.fetchall()
    return data

def run():
    new_db, broker, topic, port = initiateProgram()
    cl = repoMqtt(broker, topic, port, 60)
    data = interactDB(new_db, "query.txt")
    for ms in data:
        stack_mqtt_id, values = repo.parseData(ms)
        payload = repo.createPayload(stack_mqtt_id, values)
        print(payload)
        index = data.index(ms)
        print("Data : " + str(index))
        cl.sendPayload(payload)






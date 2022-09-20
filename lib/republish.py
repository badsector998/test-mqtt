import datetime
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
    repo.Logs("MQTT client instance has been created", "logs.txt")

    dateStart = datetime.datetime(
        year=2022,
        month=1,
        day=4,
        hour=00,
        minute=00,
        second=00
    )

    dateEnd = dateStart + datetime.timedelta(days=14)

    finalDate = datetime.datetime(
        year=2022,
        month=6,
        day=4,
        hour=19,
        minute=35,
        second=00
    )

    dbIns = db_instance(db_conf)
    repo.Logs("DB instance has been created", "logs.txt")

    while 1:

        logFileName = f"Exec {dateStart} - {dateEnd}.txt"

        query = f"""
        select stack_mqtt_id, current_parameter_name,
        current_hq_parameter_name, value, corrected_value,
        measured_at, stack_condition from measurements
        where measured_at between '{dateStart}' and '{dateEnd}'
        order by measured_at asc
        """
        data = dbIns.executeQuery(query)
        repo.Logs(
            f"Query has been executed between {dateStart} and {dateEnd}",
            logFileName
        )
        measurement = {}
        for ms in data:
            stack_mqtt_id, values = repo.parseData(ms)
            if stack_mqtt_id not in measurement:
                measurement[stack_mqtt_id] = []
            measurement[stack_mqtt_id] = append(
                                                    measurement[stack_mqtt_id],
                                                    values
                                                ).tolist()

        repo.Logs(f"List cems/stack : {measurement.keys()}", logFileName)

        for k in measurement:
            repo.Logs(
                f"Dest info : {cl.broker}, {cl.port}, {cl.topic}",
                logFileName
            )
            repo.Logs(f"sending payload for : {k}", logFileName)
            valuesLength = len(measurement[k])
            start = 0
            temp = []
            for i in range(valuesLength):
                index = i + 1
                temp = append(temp, measurement[k][i]).tolist()
                if (index % 1000 == 0) or (index == valuesLength):
                    repo.Logs(f"data from {start} to {index}", logFileName)
                    repo.Logs(f"temp values : {temp}", logFileName)
                    payload = repo.createPayload(k, temp)
                    msgLength = len(temp)
                    repo.Logs(
                        "Message length : " + str(msgLength),
                        logFileName
                    )
                    client.connect(cl.broker, cl.port, 60)
                    repo.Logs("Mqtt client connected", logFileName)
                    info = client.publish(cl.topic, payload)
                    repo.Logs("Wait for publish", logFileName)
                    info.wait_for_publish(2)
                    repo.Logs(
                        f"Published status : {info.is_published()}",
                        logFileName
                    )
                    repo.Logs("Reset temp list", logFileName)
                    temp[:] = []
                    repo.Logs("wait for next payload", logFileName)
                    start = index
                    time.sleep(60)

        if dateEnd > finalDate:
            dateEnd = finalDate
        elif dateEnd == finalDate:
            break
        else:
            dateStart = dateEnd
            dateEnd += datetime.timedelta(days=14)

    repo.Logs(
        "all payload has been sent, closing mqtt connection",
        "logs.txt"
    )
    client.disconnect()
    repo.Logs("Closing db connection", "logs.txt")
    dbIns.conn.close()
    dbIns.csr.close()
    repo.Logs("Closed db connection", "logs.txt")
    del dbIns.conn
    del dbIns.csr

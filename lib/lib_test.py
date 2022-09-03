import unittest
import repo
import db
import mqtt
from pathlib import Path


class RepoTestClass(unittest.TestCase):

    def test_ParseConf(self):
        configParser = repo.parseConfig("config-example.yaml")
        self.assertIsNotNone(configParser, "config parser caught error")

    def test_LoadConf(self):
        dbconf, api_conf = repo.loadConf("config-example.yaml")
        self.assertIsNotNone(dbconf, "load db conf error")
        self.assertIsNotNone(api_conf, "load mqtt broker conf error")

    def test_ParseData(self):
        dbconf, _ = repo.loadConf("config-example.yaml")
        dbInstance = db.db_instance(dbconf)
        data = dbInstance.executeQuery("query.txt")
        mqttIdTest, PayloadContent = repo.parseData(data)
        self.assertIsNotNone(mqttIdTest, "mqtt id is empty")
        self.assertIsNotNone(PayloadContent, "payload is empty")

    def test_CreatePayload(self):
        dbconf, _ = repo.loadConf("config-example.yaml")
        dbInstance = db.db_instance(dbconf)
        data = dbInstance.executeQuery("query.txt")
        mqttIdTest, PayloadContent = repo.parseData(data)
        Payload = repo.createPayload(mqttIdTest, PayloadContent)
        self.assertIsNotNone(Payload, "built payload is empty")

    def test_CreateDebugLog(self):
        message = "testing debug log"
        repo.createDebugLog(message)
        path = Path("log.txt")
        self.assertTrue(path.is_file(), "log file doesnt exist")


class DBTestClass(unittest.TestCase):

    def test_DB(self):
        dbconf, _ = repo.loadConf("config-example.yaml")
        dbInstance = db.db_instance(dbconf)
        dbInstance.connect()
        self.assertIsInstance(dbInstance, db.db_instance)

    def test_DBExecQuery(self):
        dbconf, _ = repo.loadConf("config-example.yaml")
        dbInstance = db.db_instance(dbconf)
        data = dbInstance.executeQuery("query.txt")
        self.assertIsNotNone(data)

    def test_DBCloseConn(self):
        dbconf, _ = repo.loadConf("config-example.yaml")
        dbInstance = db.db_instance(dbconf)
        conn = dbInstance.connect()
        self.assertIsNone(dbInstance.CloseDBInstance(conn))


class MqttTestClass(unittest.TestCase):

    def test_MqttClient(self):
        _, api_conf = repo.loadConf("config-example.yaml")
        broker = api_conf['broker']
        topic = api_conf['topic']
        port = api_conf['port']
        mqttClient = mqtt.repoMqtt(broker, topic, port, 60)
        self.assertIsNotNone(mqttClient)

    def test_MqttSendPayload(self):
        _, api_conf = repo.loadConf("config-example.yaml")
        broker = api_conf['broker']
        topic = api_conf['topic']
        port = api_conf['port']
        mqttClient = mqtt.repoMqtt(broker, topic, port, 60)
        dbconf, _ = repo.loadConf("config-example.yaml")
        dbInstance = db.db_instance(dbconf)
        data = dbInstance.executeQuery("query.txt")
        mqttIdTest, PayloadContent = repo.parseData(data)
        Payload = repo.createPayload(mqttIdTest, PayloadContent)
        self.assertIsInstance(
                                mqttClient.sendPayload(Payload),
                                mqtt.mqtt.MQTTMessageInfo
                            )


unittest.main()

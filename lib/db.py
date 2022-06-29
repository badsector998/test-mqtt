
import psycopg2

class db_instance:
    def __init__(self, conf) -> None:
        self.db = conf['db']
        self.host = conf['host']
        self.user = conf['user']
        self.pswd = conf['password']
        self.port = conf['port']

    def connect(self):
        connection = psycopg2.connect(
                                        database = self.db,
                                        host = self.host,
                                        user = self.user,
                                        password = self.pswd,
                                        port = self.port
                                     )
        return connection

    def executeQuery(self, queryText):
        connection = self.connect()
        cursor = connection.cursor()
        query_string = openQuery(queryText)
        cursor.execute(query_string)
        data = cursor.fetchall()
        return data

def openQuery(file):
    with open(file, 'r') as stream:
        query = stream.read().replace('\n', ' ')
    return query
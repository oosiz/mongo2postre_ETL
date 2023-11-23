import psycopg2 as pgsql
import pymongo as mongo


class DBConnector:

    def __init__(self, host, port, user, pw, db, location, engine):
        # print('<< init >>')
        self._host = host
        self._port = port
        self._user = user
        self._pw = pw
        self._db = db
        self._location = location
        self._engine = engine
        self.conn = None
        # print('init first self.conn : ', self.conn)

        if location == 'localhost_source':
            if engine == 'postgre':
                # print('func pgsql_connector : ', self.pgsql_connector)
                # print('func mongo_connector : ', self.mongo_connector)
                self.enter_connect = self.pgsql_connector
                # print('init second self.conn : ', self.conn)
            elif engine == 'mongodb':
                # print('func pgsql_connector : ', self.pgsql_connector)
                # print('func mongo_connector : ', self.mongo_connector)
                self.enter_connect = self.mongo_connector
                # print('init second self.conn : ', self.conn)
            else:
                raise RuntimeError(f"{engine} is not supported")

        elif location == 'localhost_target':
            if engine == 'postgre':
                self.enter_connect = self.pgsql_connector
            elif engine == 'mongodb':
                self.enter_connect = self.mongo_connector
            else:
                raise RuntimeError(f"{engine} is not supported")

        else :
            raise RuntimeError(f"{location} is not supported")

    def __enter__(self):
        
        self.enter_connect()
        print('<< DBConnection >>')
        print('\tConnection Info : ', self.conn)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # print('<< exit >>')
        try :
            self.conn.close()
        except:
            pass
        # self.conn.close()
        # self.get_collection.close()

    def pgsql_connector(self):
        print('<< pgsql_connector >>')
        self.conn = pgsql.connect(host = self._host, port = self._port, user = self._user, password = self._pw, dbname = self._db)

    def mongo_connector(self):
        print('<< mongo_connector >>')
        self.conn = mongo.mongo_client.MongoClient(host = self._host, port = self._port, username = self._user, password = self._pw).get_database(self._db)
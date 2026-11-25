from pgcompat import connect


class SimpleConnectionPool:
    def __init__(self, minconn=1, maxconn=1, *args, **kwargs):
        pass

    def getconn(self):
        return connect()

    def putconn(self, conn):
        conn.close()

    def closeall(self):
        pass


ThreadedConnectionPool = SimpleConnectionPool

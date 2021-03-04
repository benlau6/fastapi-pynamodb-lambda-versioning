from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

def connect_mongodb(host, port, username, password, db, authMechanism, authSource):
    try:
        client = MongoClient(host, port)
        # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster')
        print("Server available")
    except ConnectionFailure:
        print("Server not available")
    if username and password:
        uri = "mongodb://%s:%s@%s:%s/%s?authMechanism=%s&authSource=%s" % (username,password,host,port,db,authMechanism,authSource)
        client = MongoClient(uri)
    else:
        client = MongoClient(host,port)
    return client[db]

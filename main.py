from datetime import timedelta
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import (ClusterOptions, ClusterTimeoutOptions,QueryOptions)


def upsert_document(doc):
    print("\nUpsert CAS: ")
    try:
        key = doc["type"]+"_"+str(doc["id"])
        result = cb_coll.upsert(key,doc)
        print(result.cas)
    except Exception as e:
        print(e)

def get_airline_by_key(key):
    print("\nGet result: ")
    try:
        result = cb_coll.get(key)
        print(result.content_as[str])
    except Exception as e:
        print(e)

def lookup_by_callsign(cs):
    print("\nLookup Result: ")
    try:
        sql_query = 'SELECT VALUE name FROM `travel-sample`.inventory.airline WHERE callsign = $1'
        row_iter = cluster.query(
            sql_query,
            QueryOptions(positional_parameters=[cs]))
        for row in row_iter:
            print(row)
    except Exception as e:
        print(e)


# Inputs
username = "fabs_user"
password = "password"
bucket_name= "travel-sample"
auth = PasswordAuthenticator(username,password)

# Open cluster
cluster = Cluster("couchbase://localhost", ClusterOptions(auth))
cb = cluster.bucket(bucket_name)
cb_coll = cb.scope("inventory").collection("airline")
print("Setup done")


airline = {
    "type": "airline",
    "id": 8091,
    "callsign": "CBS",
    "iata": None,
    "icao": None,
    "name": "Couchbase Airways",
}


upsert_document(airline)
get_airline_by_key("airline_8091")
lookup_by_callsign("CBS")



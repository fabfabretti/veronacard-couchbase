from couchbase.auth import PasswordAuthenticator
from couchbase.bucket import Bucket
from couchbase.cluster import Cluster
from couchbase.options import (ClusterOptions, ClusterTimeoutOptions, QueryOptions)
import csv


def execute_qry(qry:str, cluster:Cluster):
    try:
        return cluster.query(qry).execute()
    except Exception as e:
        print (e)

def parse_card_type(type_string:str):
    if "24" in type_string:
        return "24"
    if "48" in type_string:
        return "48"
    if "72" in type_string:
        return "72"
    else:
        raise Exception("Couldn't translate type string to type.")

username = ""
password = ""

with open("login.txt") as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        username = row[0]
        password= row[1]


def connect_to_db() -> (Bucket, Cluster):
    cluster = Cluster("couchbase://localhost", ClusterOptions(PasswordAuthenticator(username,password)))
    cb = cluster.bucket("veronacard")
    print("* Connection setup done")
    return cb, cluster


def flush_collections(cluster:Cluster, collectionname:str):
    print("* ["+ collectionname +"] Dropping collection if exists", end="")
    qry = "DROP COLLECTION veronacard.veronacard_db.{} IF EXISTS".format(collectionname)
    try:
        res = cluster.query(qry)
        print(res)
        res.execute()
    except Exception as e:
        print(e)
    print("* ["+ collectionname +"] Creating collection", end="")
    qry = "CREATE COLLECTION veronacard.veronacard_db.{} IF NOT EXISTS".format(collectionname)
    try:
        res = cluster.query(qry)
        print(res)
        res.execute()
    except Exception as e:
        print(e)

    # swipe_date    swipe_time  POI_name    POI_device  card_id     card_activation   card_type
    #    0             1            2           3          4             5                8

def create_primary_index(cluster:Cluster, collectionname:str):
    print("* ["+collectionname+"] Creating index")
    execute_qry("CREATE PRIMARY INDEX `#primary` ON veronacard.veronacard_db."+collectionname+"", cluster)




def reformat_date(date:str) -> str:
    if len(date)<=8:
        date = date[0:6] + "20" + date[6:]
    return date[6:]+"-"+date[3:5]+"-"+date[0:2]
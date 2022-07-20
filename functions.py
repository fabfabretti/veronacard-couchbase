from datetime import timedelta

from couchbase.auth import PasswordAuthenticator
from couchbase.bucket import Bucket
from couchbase.cluster import Cluster
from couchbase.options import (ClusterOptions, ClusterTimeoutOptions, QueryOptions)
import csv
from sql_formatter.core import format_sql


def execute_qry(qry:str, cluster:Cluster):
    try:
        opt = QueryOptions(timeout=timedelta(minutes=20)) # Needed to avoid timeout at 75 sec
        return cluster.query(qry,opt).execute()
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


def flush_collections(cluster:Cluster, collectionname:str,scopename:str):
    print("* ["+ collectionname +"] Dropping collection if exists\t", end="")
    qry = "DROP COLLECTION veronacard."+scopename+".{} IF EXISTS".format(collectionname)
    try:
        res = cluster.query(qry)
        print(res)
        res.execute()
    except Exception as e:
        print(e)
    print("* ["+ collectionname +"] Creating collection\t", end="")
    qry = "CREATE COLLECTION veronacard."+scopename+".{} IF NOT EXISTS".format(collectionname)
    try:
        res = cluster.query(qry)
        print(res)
        res.execute()
    except Exception as e:
        print(e)

    # swipe_date    swipe_time  POI_name    POI_device  card_id     card_activation   card_type
    #    0             1            2           3          4             5                8

def create_primary_index(cluster:Cluster, collectionname:str,scopename:str):
    print("* ["+collectionname+"] Creating index")
    execute_qry("CREATE PRIMARY INDEX `#primary` ON veronacard."+scopename+"."+collectionname+"", cluster)




def reformat_date(date:str) -> str:
    if len(date)<=8:
        date = date[0:6] + "20" + date[6:]
    return date[6:]+"-"+date[3:5]+"-"+date[0:2]


def generate_calendar(cluster:Cluster, cb:Bucket):
    flush_collections(cluster, "calendar","_default")
    import pandas as pd
    date1 = '2014-01-01'
    date2 = '2020-12-31'
    mydates = pd.date_range(date1, date2).tolist()
    for date in mydates:
        date_time_obj = date.to_pydatetime()
        date = date_time_obj.date()
        key = date.strftime("%Y-%m-%d")
        doc = { "date" : key}
        cb.scope("_default").collection("calendar").upsert(key, doc)
    create_primary_index(cluster,"calendar","_default")
    execute_qry("CREATE INDEX idx_date ON veronacard._default.calendar(date)",cluster)


def format_qry(qry:str):
    return format_sql(qry)\
        .replace(" and "," AND ")\
        .replace("ifmissingornull","IFMISSINGORNULL")\
        .replace(" as ", " AS ").replace(" unnest "," UNNEST ")\
        .replace(" count "," COUNT ")\
        .replace("poi","POI")\
        .replace("let ","LET")

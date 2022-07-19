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




"""
    /* assegnato un punto di interesse e un mese di un anno, 
    /* trovare per ogni giorno del mese il numero totale di accessi al POI.*/
        SELECT s.swipe_date AS date, COUNT(*) AS access_count
        FROM veronacard."""+string_db+"""."""+string_carddb+""" AS card 
            UNNEST swipes AS s
        WHERE s.POI_name == "Casa Giulietta"
        AND DATE_PART_STR(s.swipe_date, "month") = 8
        AND DATE_PART_STR(s.swipe_date, "year") = 2020
        GROUP BY s.swipe_date 
        ORDER BY s.swipe_date


        UNION
        SELECT calendar.date, 0 AS access_count
        FROM veronacard."""+string_db+""".calendar as calendar
    """
"""SELECT DISTINCT POI_name AS a_name,
    (select day0.date, day0.swipes
    from (
           select calendar1.date AS date, ARRAY_AGG({rawtable1.card_id, rawtable1.POI_name}) as swipes, count(*) as a_count
           from veronacard."""+string_db+""".calendar as calendar1 left join veronacard."""+string_db+"""."""+string_rawtable+""" as rawtable1
           on rawtable1.swipe_date = calendar1.date
           group by calendar1.date
           order by calendar1.date
           ) as day0
        ) as day unnest day.swipes as DS
    FROM veronacard."""+string_db+"""."""+string_rawtable+""" as rawtable
    where DS.POI_name = rawtable.POI_name
    GROUP BY POI_name
    order by POI_name"""


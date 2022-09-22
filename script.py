import csv
import json
import sys
from datetime import timedelta
import time
from sys import argv

from couchbase.options import QueryOptions
import functions
from sys import getsizeof

## Setup
cb, cluster = functions.connect_to_db()

 # Establishes if we're working on  full data or a smaller sample.
mini = "True" in argv[1]
if mini:
    string_db = "mini_veronacard"
    string_carddb = "mini_card"
    string_POIdb = "mini_POI"
    string_rawtable="mini_raw"
else:
    string_db = "full_veronacard"
    string_carddb = "full_card"
    string_POIdb = "full_POI"
    string_rawtable="full_raw"
years = ["2014", "2015", "2016", "2017", "2018", "2019", "2020"]

def load_raw_data():
    """
    Loads data from CSVs creating a collection and its primary index.
    :return: None
    """
    # Work on smaller dataset
    limit = 0
    if mini:
        limit = 3000

    # Generate CSV names
    file = "dataset_veronacard_2014_2020/no_header/dati_X.csv"

    # Load from CSV
    for year in years:
        file = "dataset_veronacard_2014_2020/no_header/dati_X.csv".replace("X",year)
        print("Loading "+ file)
        functions.flush_collections(cluster,string_rawtable+"_"+year,string_db)
        timer_start = time.time()
        with open(file) as csvfile:
            reader = csv.reader(csvfile)
            count = 0
            for row in reader:
                key = "_".join(["swipe",
                                row[4],
                                (functions.reformat_date(row[0]).replace("-", "")),
                                row[1].replace(":", "")])
                value = {
                    "card_id": row[4],
                    "card_type": functions.parse_card_type(row[8]),
                    "card_activation": functions.reformat_date(row[5]),
                    "POI_name": row[2],
                    "POI_device": row[3],
                    "swipe_date": functions.reformat_date(row[0]),
                    "swipe_time": row[1]
                }
                try:
                    cb.scope(string_db).collection(string_rawtable+"_"+year).upsert(key, value)
                    count += 1
                    timer_curr_m = (time.time() - timer_start) / 60
                    timer_curr_s = (time.time() - timer_start) % 60
                    print("\r[{:.0f}:{:2.0f}] Progress: {} ".format(timer_curr_m, timer_curr_s,count),end=" ")
                except Exception as e:
                    print(e)

                if limit != 0 and count >= limit:
                    break
        print("")
        functions.create_primary_index(cluster,string_rawtable+"_"+year,string_db)
        print("")


def aggregate_to_card():
    """
    Aggregates data, flattening everything on cards.
    :return: None
    """
    # Flush collection if exists
    functions.flush_collections(cluster, string_carddb,string_db)
    timer_start = time.time()
    for year in years:
        print("* Aggregating year {} into cards".format(year))
        qry_card = """SELECT DISTINCT card_id AS id,
               card_type AS type,
               card_activation as activation,
               ARRAY_AGG( { 
                  POI_name, 
                  swipe_date, 
                  swipe_time} 
                ) AS swipes
        FROM veronacard.veronacard_db.full_raw_table
        GROUP BY card_id,
                 card_type,
                 card_activation""".replace("veronacard_db",string_db).replace("full_raw_table",string_rawtable+"_"+year)
        #print(qry_card)
        try:
            opt = QueryOptions(timeout=timedelta(minutes=20)) # Needed to avoid timeout at 75 sec
            res = cluster.query(qry_card, opt)
            for doc in res:
                key = "card_" + doc["id"]
                cb.scope(string_db).collection(string_carddb).upsert(key,doc)
        except Exception as e:
            print(e)

    timer_curr_m = (time.time() - timer_start) / 60
    timer_curr_s = (time.time() - timer_start) % 60
    print("* Query and upserting time: {:.0f}:{:2.0f}. ".format(timer_curr_m,timer_curr_s))


    # Create primary index in order to be able to query
    functions.create_primary_index(cluster,string_carddb,string_db)
    print("* Index creation time: {:.2f} seconds.".format(time.time() - timer_start))


def aggregate_to_POI():
    """
    Aggregates data, flattening everything on POIs.
    :return: None
    """

    # Flush collection if exists    for year in years:

    timer_start = time.time()

    for year in years:
        print("* Aggregating year {} into cards".format(year))
        functions.flush_collections(cluster, string_POIdb+"_"+year ,string_db)
        qry_POI = """SELECT DISTINCT POI_name AS name,
               ARRAY_AGG({
                POI_device,
                card_id, 
                swipe_date,
                swipe_time}) AS swipes
        FROM veronacard.veronacard_db.full_raw_table
        GROUP BY POI_name""".replace("veronacard_db",string_db).replace("full_raw_table",string_rawtable+"_"+year)
        #print(qry_POI)
        try:
            opt = QueryOptions(timeout=timedelta(minutes=20)) # Needed to avoid timeout at 75 sec
            res = cluster.query(qry_POI, opt)
            # Insert all results into POI table
            [cb.scope(string_db).collection(string_POIdb+"_"+year).upsert("POI_" + doc["name"].replace(" ",""),doc) for doc in res]
        except Exception as e:
            print (e)
        functions.create_primary_index(cluster,string_POIdb+"_"+year,string_db)

    print("* Query and upserting time: {:.2f} seconds. ".format(time.time() - timer_start))

    # Create primary index in order to be able to query
    #functions.create_primary_index(cluster,string_POIdb,string_db)
    #print("* Index creation time: {:.2f} seconds.".format(time.time() - timer_start))


def query1(POI:str, month:str, year:str)->list:
    # Final version
    print("1. Assegnato un punto di interesse e un mese di un anno, trovare per ogni giorno del mese il numero totale di accessi al POI.\n---- query ----")
    qry = """SELECT calendar.date,
         IFMISSINGORNULL(counting.access_count,
        0) AS access_count
FROM 
    (SELECT S.swipe_date AS date,
        COUNT (*) AS access_count
    FROM veronacard.mini_veronacard.%collection%_2015 AS POIdb UNNEST POIdb.swipes AS S
    WHERE DATE_PART_STR(S.swipe_date, "year") = 2020
            AND DATE_PART_STR(S.swipe_date, "month") = 7
            AND POIdb.name = 'Casa Giulietta'
    GROUP BY  S.swipe_date ) AS counting
RIGHT JOIN 
    (SELECT c.date AS date
    FROM veronacard._default.calendar AS c
    WHERE DATE_PART_STR(c.date, "year") = 2020
            AND DATE_PART_STR(c.date, "month") = 7) AS calendar
    ON calendar.date == counting.date"""\
        .replace("%collection%",string_POIdb)\
        .replace("2015",year)\
        .replace("7",month)\
        .replace("Casa Giulietta",POI)

    print(functions.format_qry(qry))
    return functions.execute_qry(qry,cluster)


def query2(date:str,consider_0s:bool)-> list:
    print("\n2. Trovare il punto di interesse che ha avuto il numero minimo di accessi in un giorno assegnato.\n\n\t ---- query ----")

    year = date[0:4]
    if consider_0s:
        print("[Considering days with 0 access]")
        qry = """
        WITH swipeslist AS (SELECT poi.name AS poiname, COUNT(*) AS countedswipes
                FROM veronacard.mini_veronacard.mini_POI_2016 AS poi
                UNNEST poi.swipes AS s
                WHERE DATE_FORMAT_STR(s.swipe_date,"1111-11-11") = "2016-08-09"
                GROUP BY poi.name),
          daily_count AS (SELECT n AS name, IFMISSINGORNULL(s.countedswipes, 0) AS countedswipes
                          FROM (SELECT RAW poi.name
                                FROM veronacard.mini_veronacard.mini_POI_2016 AS poi
                                GROUP BY poi.name) AS n
                          LEFT JOIN swipeslist AS s
                          ON s.poiname = n),
              min_count AS (ARRAY_MIN(daily_count[*].countedswipes))
        SELECT d.*
        FROM daily_count AS d
        WHERE d.countedswipes = min_count"""\
            .replace("mini_veronacard", string_db)\
            .replace("full_POI_2016",string_POIdb+"_"+year)\
            .replace("2016-08-09", date)
    else:
        print("\n[NOT considering days with 0 access]")
        qry = \
        """WITH swipescount AS (
                SELECT poi.name AS poiname, COUNT (*) AS count
                FROM veronacard.mini_veronacard.mini_POI_2014 AS poi UNNEST poi.swipes AS s
                WHERE s.swipe_date = "2014-12-29"
                GROUP BY poi.name),
                min_count AS (ARRAY_MIN(swipescount[*].count))
        SELECT sc.poiname, sc.count
        FROM swipescount AS sc
        WHERE sc.count = min_count
         """.replace("mini_POI_2014",string_carddb+"_"+year).\
            replace("2019-04-10",date)
    # TODO: IF 0s ARE ADMITTED, RN IT ONLY CHECKS POIs THAT EXIST IN THAT YEAR.
    print(qry)
    return functions.execute_qry(qry,cluster)

def query3(POI1:str,POI2:str)-> list:
    qry_2exists = """SELECT card.id as card_id, ARRAY_AGG({s.POI_name,s.swipe_date, s.swipe_time}) AS swipes FROM veronacard.XXXX.YYYY as card UNNEST card.swipes AS s WHERE EXISTS ( SELECT 1 FROM veronacard.XXXX.YYYY AS card1 UNNEST card1.swipes AS s1 WHERE card1.id == card.id AND (s1.swipe_time <> s.swipe_time OR s1.swipe_date <> s.swipe_time) AND s1.POI_name == "Teatro Romano" ) AND EXISTS ( SELECT 1 FROM veronacard.XXXX.YYYY AS card2 UNNEST card2.swipes AS s2 WHERE card2.id == card.id AND (s2.swipe_time <> s.swipe_time OR s2.swipe_date <> s.swipe_time) AND s2.POI_name == "Casa Giulietta" ) GROUP BY card.id"""\
        .replace("XXXX",string_db).replace("YYYY",string_carddb)

    qry = """WITH eligibles AS (
    SELECT DISTINCT card.id AS id
    FROM veronacard.veronacard_db.full_card_db AS card UNNEST card.swipes AS s1 UNNEST card.swipes AS s2
    WHERE s1.POI_name = "Verona Tour" 
        AND s2.POI_name = "Santa Anastasia" 
        AND (s1.swipe_date <> s2.swipe_date OR s1.swipe_time <> s2.swipe_time))
    SELECT eligibles.id AS id, ARRAY_AGG({s.POI_name, s.swipe_date, s.swipe_time}) AS swipes
    FROM eligibles 
    JOIN veronacard.veronacard_db.full_card_db AS card ON card.id = eligibles.id 
        UNNEST card.swipes AS s
    GROUP BY eligibles.id""".replace("veronacard_db",string_db).replace("full_card_db",string_carddb).\
        replace("Verona Tour",POI1).replace("Santa Anastasia",POI2)
    print(functions.format_qry(qry))
    return functions.execute_qry(qry,cluster)

def query_with_formatting(query_function):
    res = query_function
    print("\n\n\t------ results -----")
    for doc in res[0:10]:
        print(json.dumps(doc,indent=2))
    print("\t------ stats -----")
    print("* {} documents".format(len(res)))

if "load" in argv[1]:
 load_raw_data()
elif "aggregate_card" in argv[1]:
    aggregate_to_card()
elif "aggregate_POI" in argv[1]:
    aggregate_to_POI()
elif "generate calendar" in argv[1]:
    functions.generate_calendar(cluster, cb)
elif "query1" in argv[1]:
    query_with_formatting(query1("Arena","7","2015"))
elif "query2" in argv[1]:
    query_with_formatting(query2("2016-08-09","0s" in argv[1]))
else:
    print("No operation selected...")



#start0 = time.time()


#end = time.time()
#print("* Query time: {:.2f} seconds.".format(end - start0))


print("done!")


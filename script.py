import csv
from datetime import timedelta
import time
from couchbase.options import QueryOptions
import functions


## Setup
cb, cluster = functions.connect_to_db()

mini = False  # Establishes if we're working on  full data or a smaller sample.
if mini == True:
    string_db = "veronacard_minidb"
    string_carddb = "mini_card_db"
    string_POIdb = "mini_POI_db"
    string_rawtable="mini_raw_table"
else:
    string_db = "veronacard_db"
    string_carddb = "full_card_db"
    string_POIdb = "full_POI_db"
    string_rawtable="full_raw_table"


def load_raw_data():
    """
    Loads data from CSVs creating a collection and its primary index.
    :return: None
    """
    # Work on smaller dataset
    limit = 0
    if mini == True:
        limit = 3000

    # Clean colelction if already exists
    functions.flush_collections(cluster,string_rawtable,string_db)

    # Generate CSV names
    file = "dataset_veronacard_2014_2020/no_header/dati_X.csv"
    files = [   #file.replace("X","2014"),
         #file.replace("X","2015"),
         file.replace("X","2016") #,
         #file.replace("X","2017"),
         #file.replace("X","2018"),
         #file.replace("X","2019"),
         #file.replace("X","2020")
    ]

    # Load from CSV
    for file in files:
        with open(file) as csvfile:
            print("\nLoading "+ file)
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
                    cb.scope(string_db).collection(string_rawtable).upsert(key, value)
                    count += 1
                    print("\rProgress: {}".format(count), end=" ")
                except Exception as e:
                    print(e)

                if limit != 0 and count >= limit:
                    break
    print(" ")
    functions.create_primary_index(cluster,string_rawtable,string_db)


def aggregate_to_card():
    """
    Aggregates data, flattening everything on cards.
    :return: None
    """

    # Flush collection if exists
    functions.flush_collections(cluster, string_carddb,string_db)

    # Use N1QL to generate new table more easilly
    start = time.time()
    qry_card = """SELECT DISTINCT card_id AS id,
           card_type AS type,
           card_activation as activation,
           ARRAY_AGG( { 
              POI_name, 
              swipe_date, 
              swipe_time} 
            ) AS swipes
    FROM veronacard."""+string_db+""".""" + string_rawtable + """
    GROUP BY card_id,
             card_type,
             card_activation"""
    print(qry_card)

    try:
        opt = QueryOptions(timeout=timedelta(minutes=10)) # Needed to avoid timeout at 75 sec
        res = cluster.query(qry_card, opt)
        for doc in res:
            key = "card_" + doc["id"]
            cb.scope(string_db).collection(string_carddb).upsert(key,doc)
    except Exception as e:
        print(e)

    end = time.time()
    print("* Query and upserting time: {:.2f} seconds. ".format(end - start))
    start = end

    # Create primary index in order to be able to query
    functions.create_primary_index(cluster,string_carddb,string_db)
    end = time.time()
    print("* Index creation time: {:.2f} seconds.".format(end - start))


def aggregate_to_POI():
    """
    Aggregates data, flattening everything on POIs.
    :return: None
    """

    # Flush collection if exists
    functions.flush_collections(cluster, string_POIdb,string_db)

    # Use N1QL to generate new table more easilly
    start = time.time()
    qry_POI = """SELECT DISTINCT POI_name AS name,
           ARRAY_AGG({
            POI_device,
            card_id, 
            swipe_date,
            swipe_time}) AS swipes
    FROM veronacard."""+string_db+""".""" + string_rawtable +"""
    GROUP BY POI_name"""
    print(qry_POI)
    try:
        opt = QueryOptions(timeout=timedelta(minutes=10)) # Needed to avoid timeout at 75 sec
        res = cluster.query(qry_POI, opt)
        for doc in res:
            key = "POI_" + doc["name"].replace(" ","")
            cb.scope(string_db).collection(string_POIdb).upsert(key,doc)
    except Exception as e:
        print (e)

    end = time.time()
    print("* Query and upserting time: {:.2f} seconds. ".format(end - start))
    start = end

    # Create primary index in order to be able to query
    functions.create_primary_index(cluster,string_POIdb,string_db)
    end = time.time()
    print("* Index creation time: {:.2f} seconds.".format(end - start))


def query1(POI:str, month:str, year:str)->list:
    # Final version
    qry = """
    SELECT calendar.date,
         IFMISSINGORNULL(counting.access_count,
        0) AS access_count
FROM 
    (SELECT S.swipe_date AS date,
        COUNT (*) AS access_count
    FROM veronacard.veronacard_db.full_POI_db AS POIdb UNNEST POIdb.swipes AS S
    WHERE DATE_PART_STR(S.swipe_date, "year") = 2020
            AND DATE_PART_STR(S.swipe_date, "month") = 7
            AND POIdb.name = 'Casa Giulietta'
    GROUP BY  S.swipe_date ) AS counting
RIGHT JOIN 
    (SELECT c.date AS date
    FROM veronacard._default.calendar AS c
    WHERE DATE_PART_STR(c.date, "year") = 2020
            AND DATE_PART_STR(c.date, "month") = 7) AS calendar
    ON calendar.date == counting.date""".replace("veronacard_db",string_db)\
        .replace("full_POI_db",string_POIdb)\
        .replace("2020",year)\
        .replace("7",month)\
        .replace("Casa Giulietta",POI)


    # V 0.1 (no projection before join)
    qry_0= """SELECT calendar.date, IFMISSINGORNULL(counting.access_count,0) AS access_count FROM ( SELECT S.swipe_date AS date,COUNT (*) AS access_count FROM veronacard."""+string_db+"""."""+string_POIdb+""" AS POIdb UNNEST POIdb.swipes as S WHERE DATE_PART_STR(S.swipe_date, "year") = 2020 AND DATE_PART_STR(S.swipe_date, "month") = 7 AND POIdb.name = 'Casa Giulietta' GROUP BY S.swipe_date ) AS counting RIGHT JOIN veronacard._default.calendar AS calendar ON calendar.date == counting.date"""
    print(functions.format_qry(qry))
    return functions.execute_qry(qry,cluster)

def query2(date:str,consider_0s:bool)-> list:
    if consider_0s:
        qry = """
        WITH swipeslist AS (SELECT poi.name AS poiname, COUNT(*) AS countedswipes
                FROM veronacard.veronacard_db.full_POI_db AS poi
                UNNEST poi.swipes AS s
                WHERE DATE_FORMAT_STR(s.swipe_date,"1111-11-11") = "2016-08-09"
                GROUP BY poi.name),
          daily_count AS (SELECT n AS name, IFMISSINGORNULL(s.countedswipes, 0) AS countedswipes
                          FROM (SELECT RAW poi.name
                                FROM veronacard.veronacard_db.full_POI_db AS poi
                                GROUP BY poi.name) AS n
                          LEFT JOIN swipeslist AS s
                          ON s.poiname = n),
              min_count AS (ARRAY_MIN(daily_count[*].countedswipes))
        SELECT d.*
        FROM daily_count AS d
        WHERE d.countedswipes = min_count""".replace("veronacard_db", string_db).replace("full_POI_db",string_POIdb).replace("2016-08-09", date)
    else:
     qry = """
        WITH swipescount AS (
            SELECT poi.name AS poiname, COUNT (*) AS count
            FROM veronacard.veronacard_db.full_POI_db AS poi UNNEST poi.swipes AS s
            WHERE DATE_FORMAT_STR(s.swipe_date,"1111-11-11") = "2019-04-10"
            GROUP BY poi.name),
            min_count AS (ARRAY_MIN(swipescount[*].count))
        SELECT sc.poiname, sc.count
        FROM swipescount AS sc
        WHERE sc.count = min_count
     """.replace("veronacard_db",string_db).replace("full_POI_db",string_carddb).replace("2019-04-10",date)

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
    #print("\t------ results -----")
    #for doc in res[0:10]:
    #    print(json.dumps(doc,indent=2))
    print("\t------ stats -----")
    print("* {} results".format(len(res)))



load_raw_data()
#aggregate_to_card()
#aggregate_to_POI()

#start0 = time.time()

#query_with_formatting(query1("Tomba Giulietta","6","2015"))
#query_with_formatting(query2("2015-04-05",False))
#query_with_formatting(query3("Verona Tour", "Santa Anastasia"))

#end = time.time()
#print("* Query time: {:.2f} seconds.".format(end - start0))

#functions.generate_calendar(cluster,cb)

print("done!")



def unused_queries():
    """
        /* assegnato un punto di interesse e un mese di un anno, 
        /* trovare per ogni giorno del mese il numero totale di accessi al POI.*/
            SELECT s.swipe_date AS date, COUNT(*) AS access_count
            FROM veronacard.""" + string_db + """.""" + string_carddb + """ AS card 
                UNNEST swipes AS s
            WHERE s.POI_name == "Casa Giulietta"
            AND DATE_PART_STR(s.swipe_date, "month") = 8
            AND DATE_PART_STR(s.swipe_date, "year") = 2020
            GROUP BY s.swipe_date 
            ORDER BY s.swipe_date


            UNION
            SELECT calendar.date, 0 AS access_count
            FROM veronacard.""" + string_db + """.calendar as calendar
        """
    """SELECT DISTINCT POI_name AS a_name,
        (select day0.date, day0.swipes
        from (
               select calendar1.date AS date, ARRAY_AGG({rawtable1.card_id, rawtable1.POI_name}) as swipes, count(*) as a_count
               from veronacard.""" + string_db + """.calendar as calendar1 left join veronacard.""" + string_db + """.""" + string_rawtable + """ as rawtable1
               on rawtable1.swipe_date = calendar1.date
               group by calendar1.date
               order by calendar1.date
               ) as day0
            ) as day unnest day.swipes as DS
        FROM veronacard.""" + string_db + """.""" + string_rawtable + """ as rawtable
        where DS.POI_name = rawtable.POI_name
        GROUP BY POI_name
        order by POI_name"""



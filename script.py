import csv
from datetime import datetime, timedelta

import time

from couchbase.options import QueryOptions

import functions

## Setup

cb, cluster = functions.connect_to_db()

mini = False  # Establishes if we're working on  full data or a smaller sample.



if mini == True:
    string_carddb = "mini_card_db"
    string_POIdb = "mini_POI_db"
    string_rawtable="mini_raw_table"
else:
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
    functions.flush_collections(cluster,string_rawtable)

    # Generate CSV names
    file = "dataset_veronacard_2014_2020/no_header/dati_X.csv"
    files = [  # file.replace("X","2014"),
         #file.replace("X","2015"),
         #file.replace("X","2016"),
         #file.replace("X","2017"),
         file.replace("X","2018"),
         file.replace("X","2019"),
         file.replace("X","2020")
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
                    cb.scope("veronacard_db").collection(string_rawtable).upsert(key, value)
                    count += 1
                    print("\rProgress: {}".format(count), end=" ")
                except Exception as e:
                    print(e)

                if limit != 0 and count >= limit:
                    break
    print(" ")
    functions.create_primary_index(cluster,string_rawtable)


def aggregate_to_card():
    """
    Aggregates data, flattening everything on cards.
    :return: None
    """

    # Flush collection if exists
    functions.flush_collections(cluster, string_carddb)

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
    FROM veronacard.veronacard_db.""" + string_rawtable + """
    GROUP BY card_id,
             card_type,
             card_activation"""
    try:
        opt = QueryOptions(timeout=timedelta(minutes=10)) # Needed to avoid timeout at 75 sec
        res = cluster.query(qry_card, opt)
        for doc in res:
            key = "card_" + doc["id"]
            cb.scope("veronacard_db").collection(string_carddb).upsert(key,doc)
    except Exception as e:
        print(e)
    end_query = time.time()
    print("\tAggregating query time: " + str(end_query-start) + " seconds")

    # Create primary index in order to be able to query
    functions.create_primary_index(cluster,string_carddb)
    end_op = time.time()
    print("\tIndex creation time: " + str(end_op-end_query) + " seconds")

def aggregate_to_POI():
    """
    Aggregates data, flattening everything on POIs.
    :return: None
    """


    # Flush collection if exists
    functions.flush_collections(cluster, string_POIdb)

    # Use N1QL to generate new table more easilly
    start = time.time()
    qry_POI = """SELECT DISTINCT POI_name AS name,
           ARRAY_AGG({
            POI_device,
            card_id, 
            swipe_date,
            swipe_time}) AS swipes
    FROM veronacard.veronacard_db.""" + string_rawtable +"""
    GROUP BY POI_name"""
    try:
        opt = QueryOptions(timeout=timedelta(minutes=10)) # Needed to avoid timeout at 75 sec
        res = cluster.query(qry_POI, opt)
        for doc in res:
            key = "POI_" + doc["name"].replace(" ","")
            print(cb.scope("veronacard_db").collection(string_POIdb).upsert(key,doc))
    except Exception as e:
        print (e)
    end_query = time.time()
    print("\tAggregating query time: " + str(end_query-start) + " seconds")

    # Create primary index in order to be able to query
    functions.create_primary_index(cluster,string_POIdb)
    end_op = time.time()
    print("\tIndex creation time: " + str(end_op-end_query) + " seconds")

"""
SELECT DISTINCT POI_name AS name,
           (
           select calendar1.date, ARRAY_AGG({rawtable1.card_id}) as swipes
           from veronacard.veronacard_db.calendar as calendar1 left join veronacard.veronacard_db.mini_raw_table as rawtable1
           on rawtable1.swipe_date = calendar1.date
           group by calendar1.date
           ) as day
    FROM veronacard.veronacard_db.mini_raw_table as rawtable
    GROUP BY POI_name
"""
"""
    /* assegnato un punto di interesse e un mese di un anno, 
    /* trovare per ogni giorno del mese il numero totale di accessi al POI.*/
        SELECT s.swipe_date AS date, COUNT(*) AS access_count
        FROM veronacard.veronacard_db.mini_card_db AS card 
            UNNEST swipes AS s
        WHERE s.POI_name == "Casa Giulietta"
        AND DATE_PART_STR(s.swipe_date, "month") = 8
        AND DATE_PART_STR(s.swipe_date, "year") = 2020
        GROUP BY s.swipe_date 
        ORDER BY s.swipe_date


        UNION
        SELECT calendar.date, 0 AS access_count
        FROM veronacard.veronacard_db.calendar as calendar
    """
"""SELECT DISTINCT POI_name AS a_name,
    (select day0.date, day0.swipes
    from (
           select calendar1.date AS date, ARRAY_AGG({rawtable1.card_id, rawtable1.POI_name}) as swipes, count(*) as a_count
           from veronacard.veronacard_db.calendar as calendar1 left join veronacard.veronacard_db.mini_raw_table as rawtable1
           on rawtable1.swipe_date = calendar1.date
           group by calendar1.date
           order by calendar1.date
           ) as day0
        ) as day unnest day.swipes as DS
    FROM veronacard.veronacard_db.mini_raw_table as rawtable
    where DS.POI_name = rawtable.POI_name
    GROUP BY POI_name
    order by POI_name"""


def query1_():
    qry = """
    /* assegnato un punto di interesse e un mese di un anno, 
    /* trovare per ogni giorno del mese il numero totale di accessi al POI.*/
    SELECT  S.swipe_date, COUNT(*) AS access_count
    FROM veronacard.veronacard_db.mini_POI_db AS POIdb UNNEST swipes as S
    WHERE DATE_PART_STR(S.swipe_date, "year") = 2020 
        AND DATE_PART_STR(S.swipe_date, "month") = 8
        AND POIdb.name = 'Casa Giulietta'
    GROUP BY S.swipe_date
    ORDER BY S.swipe_date
    """
    # results in 6 docs in mini db
    functions.execute_qry(qry,cluster)

def query2():
    qry=""" NO 0 MINIMUMS
    SELECT countedfinal.poiname1, countedfinal.countedswipes1
    FROM (
        SELECT POI1.name AS poiname1, COUNT(*) AS countedswipes1
        FROM veronacard.veronacard_db.mini_POI_db AS POI1 UNNEST POI1.swipes AS S1
            WHERE 
                DATE_PART_STR(S1.swipe_date, "year") = 2020 AND
                DATE_PART_STR(S1.swipe_date, "month") = 8 AND
                DATE_PART_STR(S1.swipe_date, "day") = 9
        GROUP BY POI1.name
        ) AS countedfinal
    WHERE countedswipes1 WITHIN (
        SELECT MIN(counted.countedswipes2)
        FROM(
            SELECT POI2.name as poiname2, COUNT(*) as countedswipes2
            FROM veronacard.veronacard_db.mini_POI_db AS POI2 UNNEST POI2.swipes AS S2
            WHERE 
                DATE_PART_STR(S2.swipe_date, "year") = 2020 AND
                DATE_PART_STR(S2.swipe_date, "month") = 8 AND
                DATE_PART_STR(S2.swipe_date, "day") = 9
            GROUP BY POI2.name) AS counted)
    """

    QRY = """SELECT daily_count.name, daily_count.countedswipes 
        FROM ( /*SUBQUERY: find swipes count on 9/8/20 for every POI*/
        SELECT POInames.name,IFMISSINGORNULL(partial_count.countedswipes,0) AS countedswipes 
       /* I need this because otherwise if the POI has no swipes, it will not be included (like this, instead, it will be included with swipescount = 0) */
        FROM (
              SELECT POI1.name AS poiname, COUNT(*) AS countedswipes
              FROM veronacard.veronacard_db.mini_POI_db AS POI1 UNNEST POI1.swipes AS S1
                WHERE 
                DATE_PART_STR(S1.swipe_date, "year") = 2020 AND
                DATE_PART_STR(S1.swipe_date, "month") = 8 AND
                DATE_PART_STR(S1.swipe_date, "day") = 9
              GROUP BY POI1.name
              ) AS partial_count
        RIGHT JOIN (SELECT DISTINCT POI.name FROM veronacard.veronacard_db.mini_POI_db AS POI) 
                    AS POInames  ON  partial_count.poiname == POInames.name
    ) AS daily_count
WHERE daily_count.countedswipes WITHIN (
    SELECT MIN (daily_count1.countedswipes)
    FROM(
        SELECT POInames.name,IFMISSINGORNULL(partial_count.countedswipes,0) AS countedswipes
        FROM ( /*SUBQUERY AGAIN*/
              SELECT POI1.name AS poiname, COUNT(*) AS countedswipes
              FROM veronacard.veronacard_db.mini_POI_db AS POI1 UNNEST POI1.swipes AS S1
                WHERE 
                DATE_PART_STR(S1.swipe_date, "year") = 2020 AND
                DATE_PART_STR(S1.swipe_date, "month") = 8 AND
                DATE_PART_STR(S1.swipe_date, "day") = 9
              GROUP BY POI1.name
              ) AS partial_count
        RIGHT JOIN (SELECT DISTINCT POI.name FROM veronacard.veronacard_db.mini_POI_db AS POI) 
                    AS POInames  ON  partial_count.poiname == POInames.name
    ) AS daily_count1
)"""
    # Results in 14 docs in mini db
    functions.execute_qry(qry,cluster)

def query3():
    qry = """"
        SELECT card.id as card_id, 
        ARRAY_AGG({s.POI_name,s.swipe_date, s.swipe_time}) 
        AS swipes
        FROM veronacard.veronacard_db.mini_card_db as card 
        UNNEST card.swipes AS s
        WHERE
        EXISTS (
            SELECT 1
            FROM veronacard.veronacard_db.mini_card_db AS card1 
         UNNEST card1.swipes AS s1
            WHERE card1.id == card.id
            AND (s1.swipe_time <> s.swipe_time 
              OR s1.swipe_date <> s.swipe_time)
            AND s1.POI_name == "Teatro Romano"
        )
        AND EXISTS (
            SELECT 1
            FROM veronacard.veronacard_db.mini_card_db AS card2
         UNNEST card2.swipes AS s2
            WHERE card2.id == card.id
            AND (s2.swipe_time <> s.swipe_time 
              OR s2.swipe_date <> s.swipe_time)
            AND s2.POI_name == "Casa Giulietta"
        )
        GROUP BY card.id"""
    # Results in 14 docs in minidb
    functions.execute_qry(qry,cluster)


# load_raw_data()
# aggregate_to_card()
aggregate_to_POI()



print("done!")


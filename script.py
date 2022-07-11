import csv

import functions

## Setup

cb, cluster = functions.connect_to_db()

mini = True # Establishes if we're working or full data or a smaller sample.

if mini == True:
    string_carddb = "mini_card_db"
    string_POIdb = "mini_POI_db"
    string_rawtable="mini_raw_table"


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
    files = [  file.replace("X","2014"),
         file.replace("X","2015"),
         file.replace("X","2016"),
         file.replace("X","2017"),
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

    # Aggregate through N1QL, then upsert back to a new collection.
    qry_card = """SELECT DISTINCT card_id AS id,
           card_type AS type,
           card_activation as activation,
           ARRAY_AGG( { 
              POI_name, 
              POI_device, 
              swipe_date, 
              swipe_time} 
            ) AS swipes
    FROM veronacard.veronacard_db.""" + string_rawtable + """
    GROUP BY card_id,
             card_type,
             card_activation"""
    try:
        res = cluster.query(qry_card)
        for doc in res:
            key = "card_" + doc["id"]
            cb.scope("veronacard_db").collection(string_carddb).upsert(key,doc)
    except Exception as e:
        print(e)

    # Add primary index
    functions.create_primary_index(cluster,string_carddb)


def aggregate_to_POI():
    """
    Aggregates data, flattening everything on POIs.
    :return: None
    """

    # Flush collection if exists
    functions.flush_collections(cluster, "mini_POI_db")
    qry_POI = """SELECT DISTINCT POI_name AS name,
           ARRAY_AGG({
            POI_device,
            card_id, 
            card_activation,
            card_type,
            swipe_date,
            swipe_time}) AS swipes
    FROM veronacard.veronacard_db.mini_raw_table
    GROUP BY POI_name"""
    try:
        res = cluster.query(qry_POI)
        for doc in res:
            key = "POI_" + doc["name"].replace(" ","")
            print(cb.scope("veronacard_db").collection("mini_POI_db").upsert(key,doc))
    except Exception as e:
        print (e)

    # Add primary index
    print("* ["+string_POIdb+"] Creating index")
    functions.create_primary_index(cluster, string_POIdb)


def query1_POIDdb():
    qry = """
    SELECT  COUNT(*) AS access_count
    FROM veronacard.veronacard_db.mini_POI_db AS POIdb UNNEST swipes as S
    WHERE S.swipe_date >= '01/01/2015' 
        AND S.swipe_date < '31/12/2015'
        AND POIdb.name = 'AMO'
    GROUP BY S.swipe_date
    """
    functions.execute_qry(qry,cluster)


#load_raw_data()
aggregate_to_card()
aggregate_to_POI()

print("done!")


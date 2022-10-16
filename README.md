# Progetto di database avanzati: queries in Couchbase

## Dataset di partenza
Il dataset di partenza scelto, non caricato su Github ma scaricabile alla pagina del corso, è `dataset_veronacard_2014_2020`. 

## Queries
Le queries assegnate sono le seguenti:
1. Assegnato un punto di interesse e un mese di un anno, trovare per ogni giorno del mese il numero totale di accessi al POI.
2. Trovare il punto di interesse che ha avuto il numero minimo di accessi in un giorno assegnato.
3. Dati due  POI, trovare i codici delle veronacard che hanno fatto strisciate nei due POI riportando tutte le strisciate fatte da quella verona card.

Il file script.py permette di eseguire tutte le operazioni necessarie, ed è possibile scegliere cosa fare passando come paramentro una stringa che contenga la parola chiave dell'operazione che si desidera. 
* `year`: permette di scegliere su quali anni operare, passando come parametri successivi una stringa con l'anno per ogni anno. E' rilevante solo per le operazioni di caricamento dei dati. Ad esempio, `script.py "load year" "2020" "2021"`. Se non specificato carica tutti gli anni
* `load`: carica i dati come swipes "grezze".
* `calendar`: genera il calendario, necessario per una delle query.
* `aggregate_card`: parte dai dati caricati grezzi e genera la collezione aggregata per carta.
* `aggregate_POI`: parte dai dati caricati grezzi e genera le collezioni aggregate per anno e POI.
* `query1`: esegue la prima query e stampa i primi 10 documenti.
* `query2`: esegue la seconda query e stampa i primi 10 documenti. Aggiungendo anche `with0` viene eseguita la versione alternativa.
* `query3`: esegue la terza query e stampa i primi 10 documenti.
* `sec_indexq1`,`sec_indexq2`,`sec_indexq3`: Passando `create` o `drop` come secondo parametro, creano o distruggono gli indici secondari relativi alla query indicata.
* `mini`: in congiunzione con qualunque altra opzione, lavora su un dataset ridotto (3000 swipes per anno) anziché sul dataset completo.

## Presentazione
La presentazione del sistema Couchbase e la descrizione delle query elaborate sono disponibili in forma di powerpoint [presso questo link](https://docs.google.com/presentation/d/13i4OFUMbxITrUo_7H_o5OucxsnegwDDTqWBMhDkQ4xA/edit?usp=sharing)

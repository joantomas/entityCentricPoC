from elasticsearch import helpers
from elasticsearch import Elasticsearch
import sys
from random import random
from random import randint
from datetime import datetime

es = Elasticsearch()
indexName = "date_time_test"
actionsPerBulk = 5000
es.indices.delete(index=indexName, ignore=[400, 404])
indexSettings = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "properties": {
            "timestamp": {
                "type": "date"
            },
            "etag": {
                "type": "keyword"
            },
            "if-none-match": {
                "type": "date",
                "format": "EEE, dd MMM YYYY HH:mm:ss z||E, d M Y H:m:s z||E, d-M-Y H:m:s z"
            },
            "if-modified-since": {
                "type": "date",
                "format": "EEE, dd MMM YYYY HH:mm:ss z||E, d M Y H:m:s z||E, dd-MMM-YY HH:mm:ss z"
            },
            "last-modified": {
                "type": "date",
                "format": "EEE, dd MMM YYYY HH:mm:ss z||E, d M Y H:m:s z"
            }
        }
    }
}
es.indices.create(index=indexName, body=indexSettings)
actions = []

fields = []

tenants = []

print(datetime.utcnow())
doc = {
    "timestamp": datetime.utcnow(),
    "etag": "A6YBI4fq0JfvF6qU4dm5vFFe4SME2HDaFAT3MG2QWV0",
    "if-none-match": "Sun, 06 Nov 1994 08:49:37 GMT",
    "if-modified-since": "Sun, 06 Nov 1994 08:49:37 GMT",
    "last-modified": "Sun, 06 Nov 1994 08:49:37 GMT"
}
action = {
    "_index": indexName,
    '_op_type': 'index',
    "_source": doc
}
actions.append(action)

doc = {
    "timestamp": datetime.utcnow(),
    "etag": "A6YBI4fq0JfvF6qU4dm5vFFe4SME2HDaFAT3MG2QWV0",
    "if-none-match": "Sun, 06 Nov 1994 08:49:37 GMT",
    "if-modified-since": "Sunday, 06-Nov-94 08:49:37 GMT",
    "last-modified": "Sun, 06 Nov 1994 08:49:37 GMT"
}
action = {
    "_index": indexName,
    '_op_type': 'index',
    "_source": doc
}
actions.append(action)

doc = {
    "timestamp": datetime.utcnow(),
    "etag": "A6YBI4fq0JfvF6qU4dm5vFFe4SME2HDaFAT3MG2QWV0",
    "if-none-match": "Sun, 06 Nov 1994 08:49:37 GMT",
    "if-modified-since": "Sunday, 06-Nov-94 08:49:37 GMT",
    "last-modified": "Sun Nov  6 08:49:37 1994"
}
action = {
    "_index": indexName,
    '_op_type': 'index',
    "_source": doc
}
actions.append(action)
helpers.bulk(es, actions)


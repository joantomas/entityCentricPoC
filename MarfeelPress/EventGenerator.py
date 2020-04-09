from elasticsearch import helpers
from elasticsearch import Elasticsearch
import sys
from random import random
from random import randint
from datetime import datetime

es = Elasticsearch()
indexName = "media_group_lifecycle"
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
            "action": {
                "type": "keyword"
            },
            "tenant": {
                "type": "keyword"
            },
            "mediagroup": {
                "type": "keyword"
            },
            "error": {
                "type": "boolean"
            }
        }
    }
}
es.indices.create(index=indexName, body=indexSettings)
actions = []

fields = []

tenants = []

print(datetime.utcnow())
for row in range(5000):
    tenant = "tenant{0}".format(randint(1, 5))
    mg = "MG{0}".format(randint(1, 20))
    eventAction = ""
    if mg + tenant in tenants:
        eventAction = "Compilation"
    else:
        eventAction = "TenantCreation"
        tenants.append(mg + tenant)

    doc = {
        "timestamp": datetime.utcnow(),
        "action": eventAction,
        "tenant": tenant,
        "mediagroup": mg,
        "error": "true" if random() < 0.8 else "false"
    }
    action = {
        "_index": indexName,
        '_op_type': 'index',
        "_source": doc
    }
    actions.append(action)
    # Flush bulk indexing action if necessary
    if len(actions) >= actionsPerBulk:
        try:
            helpers.bulk(es, actions)
        except:
            print("Unexpected error:", sys.exc_info()[0])
        del actions[0:len(actions)]
        print(row)

if len(actions) > 0:
    helpers.bulk(es, actions)

transformName = "marfeelpress_installations_kpis"
transformBody = {
    "source": {
        "index": indexName
    },
    "description": "Main KPI for MarfeelPress installation",
    "dest": {
        "index": "marfeelpress_kpi"
    },
    "pivot": {
        "group_by": {
            "mediagroup": {
                "terms": {
                    "field": "mediagroup"
                }
            },
            "tenant": {
                "terms": {
                    "field": "tenant"
                }
            }
        },
        "aggregations": {
            "tenantCreation": {
                "scripted_metric": {
                    "init_script": "state.timestamps = []",
                    "map_script": "if (doc['action'].value == 'TenantCreation'){state.timestamps.add(doc['timestamp'].value)}",
                    "combine_script": "Collections.min(state.timestamps)",
                    "reduce_script": "Collections.min(states).getMillis()"
                }
            },
            "firstCompilation": {
                "scripted_metric": {
                    "init_script": "state.timestamps = []",
                    "map_script": "if (doc['action'].value == 'Compilation'){state.timestamps.add(doc['timestamp'].value)}",
                    "combine_script": "Collections.min(state.timestamps)",
                    "reduce_script": "Collections.min(states).getMillis()"
                }
            },
            "firstCompilationOK": {
                "scripted_metric": {
                    "init_script": "state.timestamps = []",
                    "map_script": "if (doc['action'].value == 'Compilation' && !doc['error'].value){state.timestamps.add(doc['timestamp'].value)}",
                    "combine_script": "Collections.min(state.timestamps)",
                    "reduce_script": "Collections.min(states).getMillis()"
                }
            },
            "TTFM": {
                "bucket_script": {
                    "buckets_path": {
                        "start": "tenantCreation.value",
                        "end": "firstCompilation.value"
                    },
                    "script": "params.end - params.start"
                }
            },
            "FMM": {
                "bucket_script": {
                    "buckets_path": {
                        "start": "tenantCreation.value",
                        "end": "firstCompilationOK.value"
                    },
                    "script": "params.end - params.start"
                }
            }
        }
    },
    "frequency": "5m",
    "sync": {
        "time": {
            "field": "order_date",
            "delay": "60s"
        }
    }
}
es.transform.put_transform(transform_id=transformName, body=transformBody)

import os
from elasticsearch import Elasticsearch

es = Elasticsearch(hosts="http://elastic:" + os.environ.get('ES_PASSWORD') + "@localhost:9200/")

def chi_square(cat):
    return {
        "query": {
            "range": {
                "qd": {
                    "gte": cat[0],
                    "lte": cat[1]
                }
            }
        },
        "aggs": {
            "significant_times_viewed": {
                "significant_terms": {
                    "field": "times_viewed",
                    "size": 10000,
                    "chi_square": {}
                }
            }
        },
        "size": 0
    }


categories = [
    [0, 0.2],
    [0.2, 0.4],
    [0.4, 0.6],
    [0.6, 0.8],
    [0.8, 1],
]

for cat in categories:
    aggs = es.search(index="hal2", body=chi_square(cat))
    print(aggs)
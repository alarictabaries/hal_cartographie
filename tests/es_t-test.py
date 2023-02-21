import os
from elasticsearch import Elasticsearch
from scipy.stats import chisquare
import numpy as np
from scipy.stats import chi2_contingency
from scipy.stats import chi2
from scipy.stats.contingency import association
from scipy.stats import pearsonr
from scipy.stats import spearmanr
from scipy.stats import fisher_exact
from scipy import stats


es = Elasticsearch(hosts="http://elastic:" + os.environ.get('ES_PASSWORD') + "@localhost:9200/")


field = "times_viewed"

def get_aggs(param):
    aggs_query = {
        "query": {
            "match": {
                "has_abstract": param
            }
        },
        "size": 0,
        "aggs": {
            "f": {
                "range": {
                    "field": field,
                    "ranges": [
                        {"to": 50},
                        {"from": 50, "to": 100},
                        {"from": 100, "to": 250}
                    ]
                }
            }
        }
    }

    ex = False
    if ex:
        aggs_query = {
            "query": {

                "bool": {
                    "must": [
                        {
                            "match": {
                                "has_keywords": param
                            }
                        },
                        {
                            "exists": {
                                "field": "field_citation_ratio"
                            }
                        }
                    ]
                }
            },
            "size": 0,
            "aggs": {
                "f": {
                    "range": {
                        "field": field,
                        "ranges": [
                            {"to": 1},
                            {"from": 1, "to": 2},
                            {"from": 2, "to": 3},
                            {"from": 3, "to": 4},
                            {"from": 4, "to": 5},
                            {"from": 5}
                        ]
                    }
                }
            }
        }

    res = []

    r = es.search(index="hal4", body=aggs_query)
    for hit in r['aggregations']['f']['buckets']:
        res.append({"key": hit['key'], "doc_count": hit['doc_count']})

    return res


qd_ranges = [True, False]
# qd_ranges = [[0, 0.2], [0.2, 0.4], [0.4, 0.6], [0.6, 0.8], [0.8, 1]]
table = []

for r in qd_ranges:
    res = get_aggs(r)
    row = []
    for key in res:
        row.append(key['doc_count'])
    table.append(row)

table = [
    [0, 10, 100],
    [100, 10, 0]
]

print(table)

from scipy.stats import ttest_ind, ttest_rel
result = ttest_ind(table[0], table[1])
print(result)
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



es = Elasticsearch(hosts="http://elastic:" + os.environ.get('ES_PASSWORD') + "@localhost:9200/")


field = "times_viewed"

def get_aggs(qd):
    aggs_query = {
        "query": {
            "range": {
                "times_downloaded": {
                    "gte": qd[0],
                    "lte": qd[1]
                }
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

    aggs_query = {
        "query": {
            "range": {
                "qd": {
                    "gte": qd[0],
                    "lte": qd[1]
                }
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
                        {"from": 100, "to": 150},
                        {"from": 150, "to": 200},
                        {"from": 200, "to": 250},
                        {"from": 250}
                    ]
                }
            }
        }
    }


    res = []

    r = es.search(index="hal-test", body=aggs_query)
    for hit in r['aggregations']['f']['buckets']:
        res.append({"key": hit['key'], "doc_count": hit['doc_count']})

    return res


qd_ranges = [[0, 0.25], [0.25, 0.5], [0.5, 0.75], [0.75, 1]]
# qd_ranges = [[0, 0.2], [0.2, 0.4], [0.4, 0.6], [0.6, 0.8], [0.8, 1]]
qd_ranges = [[0, 0.15], [0.15, 0.3], [0.3, 0.45], [0.45, 0.6], [0.6, 0.75], [0.75, 0.9]]
qd_ranges = [[0, 0.15], [0.15, 0.3], [0.3, 0.45], [0.45, 0.6], [0.6, 0.75], [0.75, 0.9]]
# qd_ranges = [[0.3, 0.45], [0.45, 0.6], [0.6, 0.75], [0.75, 0.9]]
# qd_ranges = [[0, 50], [50, 100], [100, 150], [150, 200], [200, 250], [250, 300]]

table = []

for r in qd_ranges:
    res = get_aggs(r)
    row = []
    for key in res:
        row.append(key['doc_count'])
    table.append(row)


stat, p, dof, expected = chi2_contingency(table)
print('dof=%d' % dof)
print(expected)
# interpret test-statistic
prob = 0.95
critical = chi2.ppf(prob, dof)
print('probability=%.3f, critical=%.3f, stat=%.3f' % (prob, critical, stat))
if abs(stat) >= critical:
    print('Dependent (reject H0)')
else:
    print('Independent (fail to reject H0)')
# interpret p-value
alpha = 1.0 - prob
print('significance=%.3f, p=%.3f' % (alpha, p))
if p <= alpha:
    print('Dependent (reject H0)')
else:
    print('Independent (fail to reject H0)')

print(association(table, method="cramer"))
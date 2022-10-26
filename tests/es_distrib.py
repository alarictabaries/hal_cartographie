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


def get_aggs():
    aggs_query = {
  "query": {
    "match_all": {}
  },
  "size": 0,
  "aggs": {
    "f": {
      "significant_terms": {
        "field": "times_downloaded",
        "size": 30000
      }
    }
  }
}

    res = []

    r = es.search(index="hal-test", body=aggs_query)
    for hit in r['aggregations']['f']['buckets']:
        res.append({"key": hit['key'], "doc_count": hit['doc_count']})

    return res

table = get_aggs()
tablel = []

for row in table:
    tablel.append([row["key"], row['doc_count']])
print(tablel)

k2, p = stats.normaltest(tablel)
alpha = 1e-3
print("p = {:g}".format(p[0]))
p = 8.4713e-19
if p < alpha:  # null hypothesis: x comes from a normal distribution
    print("The null hypothesis can be rejected")
else:
    print("The null hypothesis cannot be rejected")
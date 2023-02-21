import os
import eland as ed
import pandas as pd
from sklearn import datasets
from sklearn.tree import DecisionTreeClassifier
from eland.ml import MLModel
import numpy as np

metric = "field_citation_ratio"

df = ed.DataFrame(
    es_client="http://elastic:" + os.environ.get('ES_PASSWORD') + "@localhost:9200/",
    es_index_pattern="hal4",
)
df = df.query("submittedDateY_i  < 2020")
df = df.query("submittedDateY_i  > 2016")
df = df.query(metric + ' >= 0')
# must -> shs | must_not -> stm
# df = df.es_match("ART", columns=["docType_s"])

param_t = df.es_query({"bool": {"must": [{"match": {"domain_s": "*shs*"}}]}})
param_f = df.es_query({"bool": {"must_not": [{"match": {"domain_s": "*shs*"}}]}})

param_t = param_t.sample(n=10000, random_state=0)[[metric]]
param_f = param_f.sample(n=10000, random_state=0)[[metric]]


param_t = ed.eland_to_pandas(param_t)
param_f = ed.eland_to_pandas(param_f)

group1 = param_t.values.tolist()
group2 = param_f.values.tolist()

gr1 = []
for item in group1:
    gr1.append(item[0])

gr2 = []
for item in group2:
    gr2.append(item[0])

print(np.mean(gr1))
print(np.mean(gr2))

print(np.var(gr1))
print(np.var(gr2))

from scipy.stats import ttest_ind, ttest_rel

if np.var(gr1)/4 > np.var(gr2) or np.var(gr2)/4 > np.var(gr1):
    print("welch t test")
    t, p = ttest_ind(gr1, gr2, equal_var=False)
    print("ttest_ind:            t = %g  p = %g" % (t, p))
else:
    print("student t test")
    t, p = ttest_ind(gr1, gr2, equal_var=False)
    print("ttest_ind:            t = %g  p = %g" % (t, p))


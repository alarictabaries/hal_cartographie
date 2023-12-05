import os
import eland as ed
import pandas as pd
from sklearn import datasets
from sklearn.tree import DecisionTreeClassifier
from eland.ml import MLModel
import numpy as np

metric = "times_downloaded" # times_viewed times_downloaded field_citation_ratio
param = "has_keywords" # has_abstract has_keywords has_file

df = ed.DataFrame(
    es_client="http://elastic:" + os.environ.get('ES_PASSWORD') + "@localhost:9200/",
    es_index_pattern="hal4",
)
df = df.query("submittedDateY_i  < 2018")
df = df.query("submittedDateY_i  > 2014")
df = df.query(metric + ' >= 0')
print(df.shape)
# must -> shs | must_not -> stm
# df = df.es_query({"bool": {"must": [{"match": {"domain_s.keyword": "1.shs.info"}}]}})
df = df.es_query({"bool": {"must": [{"match": {"domain_s.keyword": "0.shs"}}]}})
##### df = df.es_match("domain_s : '0.shs'")
# df = df.es_match("ART", columns=["docType_s"])

print(df.shape)
print(df["domain_s"])

param_t = df.query(param + ' == True')
param_f = df.query(param + ' == False')

param_t = param_t.sample(n=8000, random_state=0)[[param, metric, "domain_s"]]
param_f = param_f.sample(n=8000, random_state=0)[[param, metric, "domain_s"]]


param_t = ed.eland_to_pandas(param_t)
param_f = ed.eland_to_pandas(param_f)

group1 = param_t.values.tolist()
group2 = param_f.values.tolist()

gr1 = []
for item in group1:
    gr1.append(item[1])

gr2 = []
for item in group2:
    gr2.append(item[1])

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


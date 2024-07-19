from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from datetime import datetime
import calendar
import os
import time

import urllib3

from libs import dimensions
from libs import hal
import threading
from names_dataset import NameDataset

es = Elasticsearch(hosts="http://elastic:changeme@localhost:9200/")

query = {
  "query": {
    "bool": {
      "must": [
        {
          "match": {
            "contributorFullName_s": "Thèses UL"
          }
        },
        {
          "match": {
            "contributor_type": "intermediate"
          }
        }
      ]
    }
  }
}

index = "hal-2023"
count = es.count(index=index, body=query)["count"]
res_scope = scan(es, index=index, query=query, scroll="60m", clear_scroll=True)
for doc in res_scope:
    notice = doc["_source"]
    es.update(index=index, id=notice["docid"], body={"doc": {"contributor_type" : "automated"}}, refresh=True, retry_on_conflict=10)
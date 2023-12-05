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

es = Elasticsearch(hosts="http://elastic:" + os.environ.get('ES_PASSWORD') + "@localhost:9200/")

query = {
            "query" : {
                "match": {
                    "contributorFullName_s": "INSPIRE HEP"
                }
            }
        }

count = es.count(index="hal4", body=query)["count"]
res_scope = scan(es, index="hal4", query=query, scroll="60m", clear_scroll=True)
for doc in res_scope:
    notice = doc["_source"]
    es.update(index="hal4", id=notice["docid"], body={"doc": {"contributor_type" : "automated"}})
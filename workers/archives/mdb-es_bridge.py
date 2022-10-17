from elasticsearch import Elasticsearch
import os

from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
col_notices = client.hal.notices

es = Elasticsearch(hosts="http://elastic:" + os.environ.get('ES_PASSWORD') + "@localhost:9200/")

cursor = col_notices.find({}, no_cursor_timeout=True)
for notice in cursor:
    notice.pop("_id")
    res = es.index(index="hal", id=notice["docid"], document=notice)
    if res["_shards"]["successful"] == 0:
        print("Error indexing")
        print(notice)
        print("\n")

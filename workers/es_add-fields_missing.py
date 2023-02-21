import time
from datetime import datetime
import re
import os
import dateutil.parser
from dateutil.relativedelta import relativedelta
from fold_to_ascii import fold

import requests
from nested_lookup import nested_lookup
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

from dateutil import parser

from libs import qd

es = Elasticsearch(hosts="http://elastic:" + os.environ.get('ES_PASSWORD') + "@localhost:9200/")

flags = 'docid,collCode_s'


# gte = 2019
# lte = 2020
# to-do : >"2017-07-01T00:00:00Z"

query = {
    "query": {
        "bool": {
            "must_not": [
                { "exists": { "field": "collCode_s"}}
            ]
        }
    }
}

res_scope = scan(es, index="hal4", query=q, scroll="60m", clear_scroll=True)
for doc in res_scope:
    req = requests.get('https://api.archives-ouvertes.fr/search/?q=docid:' +  + '&fl=' + flags + '&sort=docid%20asc')

    if req.status_code == 200:
        data = req.json()

        if "error" in data.keys():
            print("Error: ", end=":")
            print(data["error"])

            time.sleep(60)

        if "response" in data.keys():

            res_status_ok = True

            data = data['response']
            count = data['numFound']

            for notice in data['docs']:

                if 'collCode_s' not in notice:
                    notice['collCode_s'] = None

                try:
                    res = es.update(index="hal4", id=notice["docid"], body={
                        "doc": {"collCode_s": notice["collCode_s"]}})
                except:
                    print(notice)

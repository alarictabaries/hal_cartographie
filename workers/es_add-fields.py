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

from libs import qd

es = Elasticsearch(hosts="http://elastic:" + os.environ.get('ES_PASSWORD') + "@localhost:9200/")

flags = 'docid,halId_s,publicationDateY_i,modifiedDate_tdate'

increment = 0
count = 1
rows = 10000


# gte = 2019
# lte = 2020
# to-do : >"2017-07-01T00:00:00Z"

gte = "2022-01-01T00:00:00Z"
lte = "2023-01-01T00:00:00Z"


while increment < count:

    print(time.strftime("%H:%M:%S", time.localtime()), end=' : ')
    print(increment, end="/")
    print(count)

    res_status_ok = False
    while not res_status_ok:

        req = requests.get('https://api.archives-ouvertes.fr/search/?q=&fl=' + flags + '&start=' + str(
            increment) + "&rows=" + str(rows) + "&fq=submittedDate_tdate:[" + str(gte) + " TO " + str(lte) +  "}&sort=docid%20asc")

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

                    if "modifiedDate_tdate" not in notice:
                        notice["modifiedDate_tdate"] = None
                    if "publicationDateY_i" not in notice:
                        notice["publicationDateY_i"] = None

                    try:
                        res = es.update(index="hal2", id=notice["docid"], body={"doc": {"modifiedDate_tdate": notice["modifiedDate_tdate"], "publicationDateY_i": notice["publicationDateY_i"]}})
                    except:
                        print(notice)


    increment += rows

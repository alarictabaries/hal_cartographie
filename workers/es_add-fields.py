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

from dateutil import parser


from libs import qd

es = Elasticsearch(hosts="http://elastic:" + os.environ.get('ES_PASSWORD') + "@localhost:9200/")

flags = 'docid,publicationDate_tdate,submittedDate_tdate,fileMain_s,openAccess_bool'

increment = 0
count = 1
rows = 10000


# gte = 2019
# lte = 2020
# to-do : >"2017-07-01T00:00:00Z"

gte = "2007-01-01T00:00:00Z"
lte = "2010-01-01T00:00:00Z"


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

                    if 'publicationDate_tdate' not in notice:
                        notice['publicationDate_tdate'] = None

                    if notice['publicationDate_tdate']:

                        notice["has_file"] = False
                        if "fileMain_s" in notice:
                            notice["has_file"] = True

                        deposit_delta = parser.parse(notice["submittedDate_tdate"]) - parser.parse(notice["publicationDate_tdate"])
                        if notice["has_file"] or notice["openAccess_bool"]:
                            # more than 1y
                            if deposit_delta.seconds > 31536000:
                                notice["deposit_logic"] = "archiving"
                            elif deposit_delta.seconds <= 31536000:
                                notice["deposit_logic"] = "communicating"
                        else:
                            # more than 1y
                            if deposit_delta.seconds > 31536000:
                                notice["deposit_logic"] = "censusing"
                            elif deposit_delta.seconds <= 31536000:
                                notice["deposit_logic"] = "referencing"


                    if 'deposit_logic' not in notice:
                        notice['deposit_logic'] = None


                    try:
                        res = es.update(index="hal2", id=notice["docid"], body={"doc": {"publicationDate_tdate": notice["publicationDate_tdate"], "deposit_logic": notice["deposit_logic"]}})
                    except:
                        print(notice)


    increment += rows

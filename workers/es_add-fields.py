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

flags = 'docid,*_title_s,*_abstract_s,*_keyword_s'

increment = 0
count = 1
rows = 10000


# gte = 2019
# lte = 2020
# to-do : >"2017-07-01T00:00:00Z"

gte = "2022-09-01T00:00:00Z"
lte = "2022-10-01T00:00:00Z"


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

                    if 'fr_title_s' not in notice:
                        notice['fr_title_s'] = None
                    if 'fr_abstract_s' not in notice:
                        notice['fr_abstract_s'] = None

                    if 'en_title_s' not in notice:
                        notice['en_title_s'] = None
                    if 'abstract_s' not in notice:
                        notice['en_abstract_s'] = None

                    if 'fr_keyword_s' not in notice:
                        notice['fr_keyword_s'] = None
                    if 'en_keyword_s' not in notice:
                        notice['en_keyword_s'] = None


                    try:
                        res = es.update(index="hal-test", id=notice["docid"], body={"doc": {"fr_title_s": notice["fr_title_s"], "fr_abstract_s": notice["fr_abstract_s"],
                                                                                            "en_title_s": notice["en_title_s"], "en_abstract_s": notice["en_abstract_s"],
                                                                                            "fr_keyword_s": notice["fr_keyword_s"], "en_keyword_s": notice["en_keyword_s"]}})
                    except:
                        print(notice)


    increment += rows

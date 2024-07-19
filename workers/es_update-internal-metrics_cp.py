from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from datetime import datetime
import calendar
import os
import time
import requests
import urllib3

from libs import dimensions
from libs import hal
import threading

es = Elasticsearch(hosts="http://elastic:" + os.environ.get('ES_PASSWORD') + "@localhost:9200/")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

index = "hal-2023"


def update_specific_notice(notice):

    q_contributor_simple = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match" : {
                            "authFullName_s": {
                                "query": notice["contributorFullName_s"],
                                "fuzziness": "AUTO"
                            }
                        }
                    },
                    {
                        "match": {
                            "contributorId_i": notice["contributorId_i"]
                        }
                    }
                ]
            }
        }
    }

    count = es.count(index=index, body=q_contributor_simple)["count"]
    if count > 0:
        notice["contributor_type"] = "intermediate_researcher"

    es.update(index=index, id=notice["docid"], body={"doc": {"contributor_type": notice["contributor_type"]}})


    return True


def update_notices(gte, lte):
    q = {
        "query": {
            "bool": {
                "must": {
                    "range": {
                        "submittedDate_tdate": {
                            "gte": gte,
                            "lte": lte
                        }
                    }
                },
                "must_not": {
                    "exists": {
                        "field": "behavior"
                    }
                }
            }
        }
    }

    # q = {
    #     "query": {
    #         "range": {
    #             "submittedDate_tdate": {
    #                 "gte": gte,
    #                 "lte": lte
    #             }
    #         }
    #     }
    # }

    count = es.count(index=index, body=q)["count"]
    if count == 0:
        pass
    else:
        print("Thread (start) : Processing {} notices".format(count))
        # preserve_order=True
        res_scope = scan(es, index=index, query=q, scroll="60m", clear_scroll=True)
        # "no search context found for id..."
        for doc in res_scope:
            notice = doc["_source"]
            update_specific_notice(notice)

        print("Thread (end) : Processed {} notices".format(count))


# scope...
min_submitted_year = 2008
max_submitted_year = 2008

print(time.strftime("%H:%M:%S", time.localtime()) + ": Scraping started")

step = 5
for year in range(min_submitted_year, max_submitted_year + 1):
    for month in range(1, 13):

        month_processing = True
        if month_processing:
            gte = str(year) + "-" + str(month).zfill(2) + "-01"
            upper_limit_day = calendar.monthrange(year, month)[1]
            lte = str(year) + "-" + str(month).zfill(2) + "-" + str(upper_limit_day).zfill(2)
            t = threading.Thread(target=update_notices, args=(gte, lte))
            t.start()
        else:
            for day in range(1, calendar.monthrange(year, month)[1] + 1, step):
                if (day + step) > calendar.monthrange(year, month)[1]:
                    upper_limit_day = calendar.monthrange(year, month)[1] - day
                else:
                    upper_limit_day = day + step
                if upper_limit_day != 0:
                    gte = str(year) + "-" + str(month).zfill(2) + "-" + str(day).zfill(2)
                    lte = str(year) + "-" + str(month).zfill(2) + "-" + str(upper_limit_day).zfill(2)
                    print(gte)
                    print(lte)
                    t = threading.Thread(target=update_notices, args=(gte, lte))
                    t.start()

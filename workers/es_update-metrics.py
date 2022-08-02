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

es = Elasticsearch(hosts="http://elastic:" + os.environ.get('ES_PASSWORD') + "@localhost:9200/")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def update_specific_notice(notice):
    # get HAL metrics
    hal_metrics = hal.get_metrics(notice["uri_s"])
    if "times_viewed" in hal_metrics:
        notice["times_viewed"] = hal_metrics["times_viewed"]
    if "times_downloaded" in hal_metrics:
        notice["times_downloaded"] = hal_metrics["times_downloaded"]

    # get Dimensions metrics
    if notice["doiId_s"] is not None:
        dimensions_metrics = dimensions.get_metrics(notice["doiId_s"])
        if "times_cited" in dimensions_metrics:
            notice["times_cited"] = dimensions_metrics["times_cited"]
        if "field_citation_ratio" in dimensions_metrics:
            notice["field_citation_ratio"] = dimensions_metrics["field_citation_ratio"]

    if "times_viewed" not in notice:
        notice["times_viewed"] = None
    if "times_downloaded" not in notice:
        notice["times_downloaded"] = None
    if "times_cited" not in notice:
        notice["times_cited"] = None
    if "field_citation_ratio" not in notice:
        notice["field_citation_ratio"] = None

    es.update(index="hal2", id=notice["docid"], body={
        "doc": {"times_cited": notice["times_cited"], "field_citation_ratio": notice["field_citation_ratio"],
                "times_viewed": notice["times_viewed"], "times_downloaded": notice["times_downloaded"],
                "harvested_on": datetime.now().isoformat()}})
    return True


def update_notices(gte, lte):
    q = {
        "query": {
            "bool": {
                "must_not": {
                    "exists": {
                      "field": "times_viewed"
                    },
                },
                "must": {
                    "range": {
                        "submittedDate_tdate": {
                            "gte": gte,
                            "lte": lte
                        }
                    }
                }
            }
        }
    }

    count = es.count(index="hal", body=q)["count"]
    print("Thread (start) : Processing {} notices".format(count))

    res_scope = scan(es, index="hal2", query=q, preserve_order=True, scroll="60m", clear_scroll=True)
    for doc in res_scope:
        notice = doc["_source"]
        update_specific_notice(notice)

    print("Thread (end) : Processed {} notices".format(count))


min_submitted_year = 2006
max_submitted_year = 2023

print(time.strftime("%H:%M:%S", time.localtime()) + ": Scraping started")

step = 15
for year in range(min_submitted_year, max_submitted_year):
    for month in range(1, 13):
        for day in range(1, calendar.monthrange(year, month)[1] + 1, step):
            if (day + step) > calendar.monthrange(year, month)[1]:
                upper_limit_day = calendar.monthrange(year, month)[1] - day
            else:
                upper_limit_day = day + step
            if upper_limit_day != 0:
                gte = str(year) + "-" + str(month).zfill(2) + "-" + str(day).zfill(2)
                lte = str(year) + "-" + str(month).zfill(2) + "-" + str(upper_limit_day).zfill(2)
                t = threading.Thread(target=update_notices, args=(gte, lte,))
                t.start()


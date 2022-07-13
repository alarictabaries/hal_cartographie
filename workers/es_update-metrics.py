from elasticsearch import Elasticsearch
from datetime import date, datetime

from libs import dimensions
from libs import hal
import os
import threading

es = Elasticsearch(hosts="http://elastic:" + os.environ.get('ES_PASSWORD') + "@localhost:9200/")


def update_notices(gte, lte):
    q = {
        "range": {
            "submittedDate_tdate": {
                "gte": gte,
                "lte": lte
            }
        }
    }

    count = es.count(index="hal", query=q)["count"]
    notices = es.search(index="hal", query=q, size=count)

    for notice in notices["hits"]["hits"]:

        hal_metrics = hal.get_metrics(notice["halId_s"])
        if "times_viewed" in hal_metrics:
            notice["times_viewed"] = hal_metrics["times_viewed"]
        if "times_downloaded" in hal_metrics:
            notice["times_downloaded"] = hal_metrics["times_downloaded"]

        if "doiId_s" in notice:
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

        es.update(index="hal", id=notice["docid"], body={"doc": { "times_cited": notice["times_cited"], "field_citation_ratio": notice["field_citation_ratio"],
                                                                  "times_viewed": notice["times_viewed"], "times_downloaded": notice["times_downloaded"],
                                                                  "harvested_on": datetime.now().isoformat()}})


min_submitted_year = 2012

for year in range(min_submitted_year, date.today().year + 1):
    for month in range(1, 13):
        gte = str(year) + "-" + str(month).zfill(2) + "-01"
        lte = str(year) + "-" + str(month).zfill(2) + "-31"
        t = threading.Thread(target=update_notices(), args=(gte, lte,))
        t.start()

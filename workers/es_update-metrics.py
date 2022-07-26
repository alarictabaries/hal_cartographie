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


def update_notices(gte, lte):
    q = {
        "query": {
            "range": {
                "submittedDate_tdate": {
                    "gte": gte,
                    "lte": lte
                }
            }
        }
    }

    res_scope = scan(es, index="hal2", query=q, preserve_order=True, scroll="180m")

    for doc in res_scope:
        notice = doc["_source"]

        """
        # compute type of deposit
        if notice["contributor_type"] != "self":
            notices_created = hal.count_notices("contributorId_i",
                                                int(notice["contributor_type"].split("_FacetSep_")[2]))
            if notices_created > 1000:
                notice["contributor_type"] = "automated"
        """

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


min_submitted_year = 2003
max_submitted_year = 2005

print(time.strftime("%H:%M:%S", time.localtime()) + ": scraping started")

# 36 threads per year
for year in range(min_submitted_year, max_submitted_year):

    for month in range(1, 13):
        gte = str(year) + "-" + str(month).zfill(2) + "-01"
        lte = str(year) + "-" + str(month).zfill(2) + "-10"
        t = threading.Thread(target=update_notices, args=(gte, lte,))
        t.start()

        gte = str(year) + "-" + str(month).zfill(2) + "-11"
        lte = str(year) + "-" + str(month).zfill(2) + "-20"
        t = threading.Thread(target=update_notices, args=(gte, lte,))
        t.start()

        gte = str(year) + "-" + str(month).zfill(2) + "-21"
        lte = str(year) + "-" + str(month).zfill(2) + "-" + str(calendar.monthrange(year, month)[1])
        t = threading.Thread(target=update_notices, args=(gte, lte,))
        t.start()


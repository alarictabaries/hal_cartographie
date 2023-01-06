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

    if "deleted_notice" in hal_metrics:
        # overkill security
        if hal_metrics["deleted_notice"]:
            # notice["deleted_notice"] = True
            es.update(index="hal2", id=notice["docid"], body={"doc": {"deleted": True}})
            print("Notice {} is marked as deleted".format(notice["halId_s"]))
            # es.delete(index="hal2", id=notice["docid"])
            return True

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

    # counter update if hal "Max retries exceeded with"
    if "times_viewed" in hal_metrics or "times_downloaded" in hal_metrics:
        if notice["doiId_s"] is not None:
            if "times_cited" in dimensions_metrics or "field_citation_ratio" in dimensions_metrics or 'error' in dimensions_metrics or \
                    notice["doiId_s"] is None:
                es.update(index="hal2", id=notice["docid"], body={
                    "doc": {"times_cited": notice["times_cited"],
                            "field_citation_ratio": notice["field_citation_ratio"],
                            "times_viewed": notice["times_viewed"], "times_downloaded": notice["times_downloaded"],
                            "metrics_harvested_on": datetime.now().isoformat()}})
        else:
            es.update(index="hal2", id=notice["docid"], body={
                "doc": {"times_cited": notice["times_cited"], "field_citation_ratio": notice["field_citation_ratio"],
                        "times_viewed": notice["times_viewed"], "times_downloaded": notice["times_downloaded"],
                        "metrics_harvested_on": datetime.now().isoformat()}})
    else:
        if notice["doiId_s"] is not None:
            if "times_cited" in dimensions_metrics or "field_citation_ratio" in dimensions_metrics:
                print("Could not update notice {} despite having Dimensions data".format(notice["halId_s"]))
        else:
            print("Could not update notice {}".format(notice["halId_s"]))

    return True


def update_notices(gte, lte, update_lt):
    create = True

    if create:
        q = {
            "query": {
                "bool": {
                    "must": {
                        "range": {
                            "submittedDate_tdate": {
                                "gte": gte,
                                "lt": lte
                            }
                        }
                    },
                    "must_not": {
                        "exists": {
                            # "field": "times_viewed"
                            "field": "metrics_harvested_on"
                        }
                    }
                }
            }
        }
    else:
        q = {
            "query": {

                "bool": {
                    "must": [
                        {
                            "range": {
                                "submittedDate_tdate": {
                                    "gte": gte,
                                    "lte": lte
                                }
                            }
                        },
                        {
                            "range": {
                                "metrics_harvested_on": {
                                    "lt": update_lt
                                }
                            }
                        }
                    ]
                }
            }
        }

    count = es.count(index="hal2", body=q)["count"]
    if count == 0:
        pass
    else:
        print("Thread (start) : Processing {} notices".format(count))
        # preserve_order=True
        res_scope = scan(es, index="hal2", query=q, scroll="60m", clear_scroll=True)
        # "no search context found for id..."
        try:
            for doc in res_scope:
                notice = doc["_source"]
                update_specific_notice(notice)

        except Exception as e:
            print("Update (error) : {}".format(e))

        print("Thread (end) : Processed {} notices".format(count))


# scope...
min_submitted_year = "2022-11-01"
max_submitted_year = "2022-11-"

# harvested_on before....
update_lt = "2022-11-21T17:18:02.000Z"

print(time.strftime("%H:%M:%S", time.localtime()) + ": Scraping started")

update_notices("2022-11-30", "2022-12-01", update_lt)

# step = 1
# for h in range(0, 23, step):
#     gte = "2022-11-30T" + str(h).zfill(2) + ":00:00.000Z"
#     lte = "2022-11-30T" + str(h + 1).zfill(2) + ":00:00.000Z"
#     t = threading.Thread(target=update_notices, args=(gte, lte, update_lt))
#     t.start()

# step = 1
# for day in range(1, calendar.monthrange(2022, 11)[1], step):
#     gte = max_submitted_year + str(day).zfill(2)
#     lte = max_submitted_year + str(day+1).zfill(2)
#     t = threading.Thread(target=update_notices, args=(gte, lte, update_lt))
#     t.start()

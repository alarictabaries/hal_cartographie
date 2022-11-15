from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from datetime import datetime
from dateutil import parser
import calendar
import os
import time

import urllib3

from libs import dimensions
from libs import hal
import threading
from names_dataset import NameDataset

print(time.strftime("%H:%M:%S", time.localtime()) + ": Loading names dataset...")
es = Elasticsearch(hosts="http://elastic:" + os.environ.get('ES_PASSWORD') + "@localhost:9200/")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def update_specific_notice(notice):


    # deposit logic

    deposit_delta = parser.parse(notice["submittedDate_tdate"]) - parser.parse(notice["publicationDate_tdate"])
    if notice["has_file"] or notice["openAccess_bool"]:
        # more than 1y
        if deposit_delta.seconds > 31536000:
            notice["deposit_logic"] = "archiving"
        elif deposit_delta.seconds <= 31536000:
            notice["deposit_logic"] = "direct_comm"
    else:
        # more than 1y
        if deposit_delta.seconds > 31536000:
            notice["deposit_logic"] = "referencing"
        elif deposit_delta.seconds <= 31536000:
            notice["deposit_logic"] = "listing"

    es.update(index="hal2", id=notice["docid"], body={
        "doc": {"submitted_since_nrt": notice["submitted_since_nrt"]}})

    return True


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
                        "field": "contributor_type_processed.keyword"
                    }
                }
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


min_submitted_year = 2004
max_submitted_year = 2004

print(time.strftime("%H:%M:%S", time.localtime()) + ": Process started")

step = 1
for year in range(min_submitted_year, max_submitted_year + 1):
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

from pymongo import MongoClient
from datetime import date, datetime
import calendar

import urllib3

import time
from libs import dimensions
from libs import hal
import threading

client = MongoClient('mongodb://localhost:27017/')
col_notices = client.hal.notices

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def update_notices(gte, lte):

    cursor = col_notices.find({"submittedDate_tdate":
        {
            "$gte": datetime.strptime(gte, '%Y-%m-%d'),
            "$lte": datetime.strptime(lte, '%Y-%m-%d')
        }
    }, no_cursor_timeout=True)

    for notice in cursor:

        # compute type of deposit
        if notice["contributor_type"] != "self":
            notices_created = hal.count_notices("contributorId_i",
                                                int(notice["contributor_type"].split("_FacetSep_")[2]))
            if notices_created > 1000:
                notice["contributor_type"] = "automated"

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

        col_notices.update_one({"docid": notice["docid"]}, {"$set": {
            "times_cited": notice["times_cited"], "field_citation_ratio": notice["field_citation_ratio"],
            "times_viewed": notice["times_viewed"], "times_downloaded": notice["times_downloaded"],
            "contributor_type": notice["contributor_type"],
            "contributor_type_ex": notice["contributor_type"],
            "harvested_on": datetime.now()
        }})

    cursor.close()


min_submitted_year = 2002
max_submitted_year = 2005

for year in range(min_submitted_year, max_submitted_year):

    for month in range(1, 13):
        gte = str(year) + "-" + str(month).zfill(2) + "-01"
        lte = str(year) + "-" + str(month).zfill(2) + "-15"
        t = threading.Thread(target=update_notices, args=(gte, lte,))
        t.start()

        gte = str(year) + "-" + str(month).zfill(2) + "-16"
        lte = str(year) + "-" + str(month).zfill(2) + "-" + str(calendar.monthrange(year, month)[1])
        t = threading.Thread(target=update_notices, args=(gte, lte,))
        t.start()

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


def get_contributorId(notice):
    res_status_ok = False
    while not res_status_ok:
        req = requests.get('https://api.archives-ouvertes.fr/search/?q=docid:' + notice["docid"] + "&fl=docid,contributorId_i&sort=docid%20asc")
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

                # unit treatment
                for notice in data['docs']:

                    if "contributorId_i" in notice:
                        return notice["contributorId_i"]
                    else:
                        return None
        else:
            time.sleep(3)

def update_specific_notice(notice):

    if not "contributor_s" in notice:
        notice["contributorId_i"] = get_contributorId(notice)

    if notice["contributorId_i"] is not None:
        # get doc deposited by same contributor
        q_contributor = {
            "query" : {
                "match": {
                    "contributorId_i": notice["contributorId_i"]
                }
            },
            "size": 0,
            "aggs": {
                "years": {
                    "terms": { "field": "submittedDateY_i", "size": 20 }
                }
            }
        }

        f_q_contributor = {
            "query" : {
                "match": {
                    "contributorId_i": notice["contributorId_i"]
                }
            }
        }

        count = es.count(index="hal2", body=f_q_contributor)["count"]
        notice["behavior"] = "one-shot"

        has_previous = False
        has_next = False

        if "submittedDateY_i" in notice:
            original_year = int(notice["submittedDateY_i"])
        else:
            original_year = int(notice["submittedDate_tdate"][0:4])

        # current_year = 2021
        # years = []
        # for y in range(original_year+1, current_year + 1):
        #     years.append({"year": y, "trigger": False})

        if count > 1:
            aggs = es.search(index="hal2", body=q_contributor)
            print(aggs)

            for agg in aggs["aggregations"]["years"]["buckets"]:
                scope_year = int(agg["key"])
                print(scope_year)
                # if int(doc["submittedDateY_i"]) < original_year in years:
                #     for y in years:
                #         if y["year"] == int(doc["submittedDateY_i"]):
                #             y["trigger"] = True
                if original_year > scope_year >= original_year - 2:
                    has_previous = True
                elif original_year < scope_year <= original_year + 2:
                    has_next = True

        # regular = True
        # for y in years:
        #     if y["trigger"] is False:
        #         regular = False

        # outgoing
        if has_previous is True and has_next is False:
            notice["behavior"] = "outgoing"
        # ingoing
        elif has_previous is False and has_next is True:
            notice["behavior"] = "ingoing"
        # regular
        elif has_previous is True and has_next is True:
            notice["behavior"] = "regular"
        # new
        elif has_previous is False and has_next is False:
            notice["behavior"] = "one-shot"

        print(notice["behavior"])

    else:
        print("No contributorId_i for docid: {}".format(notice["docid"]))

    return True


def update_notices():
    q = {
        "query" : {
            "match": {
                "docid": "45165"
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


update_notices()
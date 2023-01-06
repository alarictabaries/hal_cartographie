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
        try:
            url = 'https://api.archives-ouvertes.fr/search/?q=docid:' + notice["docid"] + '&fl=docid,contributorId_i'
            req = requests.get(url)
            data = req.json()

            if "error" in data.keys():
                print("Error: ", end=":")
                print(url)
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
        except Exception as e:
            print(e)
            time.sleep(60)
    return None

def update_specific_notice(notice):

    # if not "contributor_s" in notice:
    #    notice["contributorId_i"] = get_contributorId(notice)
    desambiguateContributorType = False
    if desambiguateContributorType:
        if "contributorId_i" in notice and notice["contributor_type"] == "intermediate":

            q_contributor_simple = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match": {
                                    "authFullName_s": notice["contributorFullName_s"]
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

            count = es.count(index="hal4", body=q_contributor_simple)["count"]
            if count > 0:
                notice["contributor_type"] = "intermediate_author"
            else:
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

                count = es.count(index="hal4", body=q_contributor_simple)["count"]
                if count > 0:
                    notice["contributor_type"] = "intermediate_author"


    if "contributorId_i" in notice:
        # get doc deposited by same contributor
        q_contributor = {
            "query": {
                "match": {
                    "contributorId_i": notice["contributorId_i"]
                }
            },
            "size": 0,
            "aggs": {
                "years": {
                    "terms": {"field": "submittedDateY_i", "size": 20}
                }
            }
        }

        f_q_contributor = {
            "query": {
                "match": {
                    "contributorId_i": notice["contributorId_i"]
                }
            }
        }

        count = es.count(index="hal4", body=f_q_contributor)["count"]

        notice["behavior"] = "one-shot"

        has_previous = False
        has_next = False

        if "submittedDateY_i" in notice:
            original_year = int(notice["submittedDateY_i"])

            # current_year = 2021
            # years = []
            # for y in range(original_year+1, current_year + 1):
            #     years.append({"year": y, "trigger": False})

            if count > 1:
                aggs = es.search(index="hal4", body=q_contributor)

                for agg in aggs["aggregations"]["years"]["buckets"]:
                    scope_year = int(agg["key"])
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

            es.update(index="hal4", id=notice["docid"], body={"doc": {"behavior": notice["behavior"], "contributorId_i": notice["contributorId_i"], "contributor_type": notice["contributor_type"]}})

    # else:
    # es.update(index="hal4", id=notice["docid"], body={"doc": {"deleted": True}})

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

    count = es.count(index="hal4", body=q)["count"]
    if count == 0:
        pass
    else:
        print("Thread (start) : Processing {} notices".format(count))
        # preserve_order=True
        res_scope = scan(es, index="hal4", query=q, scroll="60m", clear_scroll=True)
        # "no search context found for id..."
        for doc in res_scope:
            notice = doc["_source"]
            update_specific_notice(notice)

        print("Thread (end) : Processed {} notices".format(count))


# scope...
min_submitted_year = 2001
max_submitted_year = 2022

print(time.strftime("%H:%M:%S", time.localtime()) + ": Scraping started")

step = 10
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
                    t = threading.Thread(target=update_notices, args=(gte, lte))
                    t.start()

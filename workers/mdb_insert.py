import time
from datetime import datetime

import requests
from fold_to_ascii import fold
from fuzzywuzzy import fuzz
from nested_lookup import nested_lookup
from pymongo import MongoClient

from libs import utils, qd

client = MongoClient('mongodb://localhost:27017/')
col_notices = client.hal.notices

flags = 'docid,halId_s,doiId_s,openAccess_bool,authIdHal_s,submittedDate_tdate,modifiedDate_tdate' \
        'fileMain_s,title_s,*_abstract_s,*_keyword_s,fulltext_t,domain_s,primaryDomain_s,docType_s,labStructIdName_fs,' \
        'conferenceEndDate_tdate,conferenceStartDate_tdate,defenseDate_tdate,ePublicationDate_tdate,' \
        'producedDate_tdate,publicationDate_tdate,releasedDate_tdate,writingDate_tdate,instStructIdName_fs,' \
        'submittedDateY_i,modifiedDateY_i,contributorId_i,contributorFullName_s,authFullName_s,structAddress_s,' \
        'authIdHasStructure_fs,structCode_s,structIdName_fs,structHasAlphaAuthId_fs,fileMain_s'

increment = 0
count = 1
rows = 1000

while increment < count:

    print(time.strftime("%H:%M:%S", time.localtime()), end=' : ')
    print(increment, end="/")
    print(count)

    res_status_ok = False

    while not res_status_ok:

        req = requests.get('https://api.archives-ouvertes.fr/search/?q=*&fl=' + flags + '&start=' + str(
            increment) + "&rows=" + str(rows))

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

                    if "modifiedDateY_i" not in notice:
                        notice["modifiedDateY_i"] = notice["submittedDateY_i"]

                    # author treatment
                    notice["contributor_type"] = 0
                    contributor = "-1"

                    if 'authFullName_s' in notice and 'contributorFullName_s' in notice:
                        for auth in notice["authFullName_s"]:
                            if fuzz.ratio(fold(auth), fold(notice["contributorFullName_s"])) > 70:
                                contributor = auth
                                notice["contributor_type"] = 1

                    if contributor == "-1":
                        if 'contributorFullName_s' in notice:
                            res = utils.is_a_name(notice["contributorFullName_s"])
                            if not res:
                                notice["contributor_type"] = 2

                    # QD
                    notice["qd"] = round(qd.calculate(notice), 4)

                    # get QD parameters individually
                    notice["has_file"] = False
                    if "fileMain_s" in notice:
                        notice["has_file"] = True

                    if 'publicationDate_tdate' in notice \
                            or 'conferenceStartDate_tdate' in notice \
                            or 'conferenceEndDate_tdate' in notice \
                            or 'defenseDate_tdate' in notice \
                            or 'ePublicationDate_tdate' in notice \
                            or 'producedDate_tdate' in notice \
                            or 'releasedDate_tdate' in notice \
                            or 'writingDate_tdate':
                        has_publication_date = True
                    else:
                        has_publication_date = False

                    keywords = nested_lookup(
                        key="_keyword_s",
                        document=notice,
                        wild=True,
                        with_keys=True,
                    )

                    if len(keywords) > 0:
                        notice["has_keywords"] = True
                    else:
                        notice["has_keywords"] = False

                    abstract_penalty = False

                    abstracts = nested_lookup(
                        key="_abstract_s",
                        document=notice,
                        wild=True,
                        with_keys=True,
                    )

                    sub_abstract_penalty = 0
                    for abstract in abstracts:

                        title_words_count = len(notice["title_s"][0].split())

                        if (len(notice[abstract][0].split()) < 3) or (
                                len(notice[abstract][0].split()) < title_words_count):
                            sub_abstract_penalty += 1

                    if sub_abstract_penalty == len(abstracts) and (sub_abstract_penalty != 0):
                        abstract_penalty = True

                    if len(abstracts) > 0 and not abstract_penalty:
                        notice["has_abstract"] = True
                    else:
                        notice["has_abstract"] = False

                    """
                    # get metrics
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
                    """

                    # filters
                    if "domain_s" not in notice:
                        notice["domain_s"] = None
                    if "instStructIdName_fs" not in notice:
                        notice["instStructIdName_fs"] = None
                    """
                    if "times_viewed" not in notice:
                        notice["times_viewed"] = None
                    if "times_downloaded" not in notice:
                        notice["times_downloaded"] = None
                    if "times_cited" not in notice:
                        notice["times_cited"] = None
                    if "field_citation_ratio" not in notice:
                        notice["field_citation_ratio"] = None
                    """
                    if 'doiId_s' not in notice:
                        notice["doiId_s"] = None
                    if "publicationDateY_i" not in notice:
                        notice["publicationDateY_i"] = None
                    if "modifiedDate_tdate" not in notice:
                        notice["modifiedDate_tdate"] = None
                    else:
                        notice["modifiedDate_tdate"] = datetime.fromisoformat(notice["modifiedDate_tdate"][:-1])
                    if "submittedDate_tdate" not in notice:
                        notice["submittedDate_tdate"] = None
                    else:
                        notice["submittedDate_tdate"] = datetime.fromisoformat(notice["submittedDate_tdate"][:-1])
                    if "labStructIdName_fs" not in notice:
                        notice["labStructIdName_fs"] = None
                    if "authIdHal_s" not in notice:
                        notice["authIdHal_s"] = None

                    # formatter
                    notice["inst_name"] = []
                    notice["lab_name"] = []

                    if notice["instStructIdName_fs"] is not None:
                        for inst in notice["instStructIdName_fs"]:
                            notice["inst_name"].append(inst.split("_FacetSep_")[1])
                    else:
                        notice["inst_name"] = None
                    if notice["labStructIdName_fs"] is not None:
                        for lab in notice["labStructIdName_fs"]:
                            notice["lab_name"].append(lab.split("_FacetSep_")[1])
                    else:
                        notice["lab_name"] = None

                    notice_short = {
                        "docid": notice["docid"],

                        "halId_s": notice["halId_s"],
                        "docType_s": notice["docType_s"],

                        "instStructIdName_fs": notice["instStructIdName_fs"],
                        "labStructIdName_fs": notice["labStructIdName_fs"],

                        "inst_name": notice["inst_name"],
                        "lab_name": notice["lab_name"],

                        "authIdHal_s": notice["authIdHal_s"],

                        "submittedDate_tdate": notice["submittedDate_tdate"],
                        "modifiedDate_tdate": notice["modifiedDate_tdate"],

                        "publicationDateY_i": notice["publicationDateY_i"],
                        "submittedDateY_i": notice["submittedDateY_i"],
                        "modifiedDateY_i": notice["modifiedDateY_i"],

                        "qd": notice["qd"],

                        "contributor_type": notice["contributor_type"],

                        "domain_s": notice["domain_s"],
                        "primaryDomain_s": notice["primaryDomain_s"],

                        "doiId_s": notice["doiId_s"],

                        "openAccess_bool": notice["openAccess_bool"],

                        "has_file": notice["has_file"],
                        "has_abstract": notice["has_abstract"],
                        "has_keywords": notice["has_keywords"],

                        "harvested_on": datetime.now()
                    }

                    """
                    for key in notice_short.copy():
                        if notice_short[key] == "NULL":
                            del notice_short[key]
                    """

                    res = col_notices.insert_one(notice_short).inserted_id

    increment += rows

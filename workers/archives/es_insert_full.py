import time
from datetime import datetime
import re
import os
import dateutil.parser
from dateutil.relativedelta import relativedelta
from fold_to_ascii import fold

import requests
from nested_lookup import nested_lookup
from elasticsearch import Elasticsearch

from libs import qd

from names_dataset import NameDataset


nd = NameDataset()
es = Elasticsearch(hosts="http://elastic:" + os.environ.get('ES_PASSWORD') + "@localhost:9200/")

flags = '*'

increment = 0
count = 1
rows = 2000


# gte = 2019
# lte = 2020
# to-do : >"2017-07-01T00:00:00Z"

gte = "2006-01-01T00:00:00Z"
lt = "20010-01-01T00:00:00Z"

def is_name(name):
    names_banlist = ["project", "migration", "imt", "service", "institutional", "repository", "bibliotheque", "archive",
                     "bibliothèque", "bmc", "elsevier", "institution", "institut", "archive", "migration", "irsn",
                     "images", "gestionnaire", "comité", "hal", "arxiv", "administrateur", "abes", "projet", "abes",
                     "import", "ifsttar", "centre", "documentation", "abes", "compte", "univ", " thèses",
                     "publications", "projet", "laboratoire"]

    doubt = False

    for word in name.split(" "):
        if word.lower() in names_banlist:
            return False
    for word in name.split(" "):
        if len(word) > 1:
            # ex : "a. tabaries"
            if word[1] == "." and len(name) == 2:
                doubt = True
        result = nd.search(word)
        if result["first_name"] is not None or result["last_name"] is not None:
            return True
    if doubt:
        return True
    else:
        return False


while increment < count:

    print(time.strftime("%H:%M:%S", time.localtime()), end=' : ')
    print(increment, end="/")
    print(count)

    res_status_ok = False
    while not res_status_ok:

        req = requests.get('https://api.archives-ouvertes.fr/search/?q=*&fl=' + flags + '&start=' + str(
            increment) + "&rows=" + str(rows) + "&fq=submittedDate_tdate:[" + str(gte) + " TO " + str(lt) +  "}&sort=docid%20asc")

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
                        notice["modifiedDateY_i"] = None

                    # author treatment
                    if notice["selfArchiving_bool"] is True:
                        notice["contributor_type"] = "self"
                        notice["contributor_type_processed"] = "self"
                    else:
                        hal_error = False
                        if "authFullName_s" in notice:
                            for name in notice["authFullName_s"]:
                                if fold(name.lower()) in fold(notice["contributorFullName_s"].lower()):
                                    notice["contributor"] = "self"
                                    hal_error = True
                        if hal_error is False:
                                notice["contributor"] = "other_FacetSep_" + str(notice["contributorFullNameId_fs"])

                                # process contributor name (or alias)
                                notice["contributor_type"] = None
                                name = notice["contributor"].split("_FacetSep_")[1]
                                contributor_type_processed = is_name(name)
                                if contributor_type_processed is True:
                                    notice["contributor_type"] = "intermediate"
                                else:
                                    notice["contributor_type"] = "automated"

                    # QD
                    notice["qd"] = round(qd.calculate(notice), 4)

                    # SHERPA/RoMEO embargo
                    notice["postprint_embargo"] = None
                    if 'fileMain_s' not in notice or notice["openAccess_bool"] is False or "linkExtUrl_s" not in notice:
                        if "journalSherpaPostPrint_s" in notice:
                            if notice['journalSherpaPostPrint_s'] == 'can':
                                notice["postprint_embargo"] = "false"
                            elif notice['journalSherpaPostPrint_s'] == 'restricted' and "publicationDate_tdate" in notice and "journalSherpaPostRest_s" in notice:
                                matches = re.finditer('(\S+\s+){2}(?=embargo)', notice["journalSherpaPostRest_s"].replace('[', ' '))
                                for match in matches:
                                    duration = match.group().split(' ')[0]
                                    if duration.isnumeric():
                                        publication_date = dateutil.parser.parse(notice["publicationDate_tdate"]).replace(tzinfo=None)

                                        curr_date = datetime.now()
                                        age = relativedelta(curr_date, publication_date)
                                        age_in_months = age.years * 12 + age.months

                                        if age_in_months > int(duration):
                                            notice["postprint_embargo"] = "false"
                                        else:
                                            notice["postprint_embargo"] = "true"
                            elif notice["journalSherpaPostPrint_s"] == 'cannot':
                                notice["postprint_embargo"] = "true"
                            else:
                                notice["postprint_embargo"] = None

                    notice["preprint_embargo"] = None
                    if 'fileMain_s' not in notice or notice["openAccess_bool"] is False or "linkExtUrl_s" not in notice:
                        if "journalSherpaPrePrint_s" in notice:
                            if notice['journalSherpaPrePrint_s'] == 'can':
                                notice["preprint_embargo"] = "false"
                            elif notice['journalSherpaPrePrint_s'] == 'restricted' and "journalSherpaPreRest_s" in notice:
                                if "Must obtain written permission from Editor" in notice["journalSherpaPreRest_s"]:
                                    notice["preprint_embargo"] = "perm_from_editor"
                            elif notice["journalSherpaPrePrint_s"] == 'cannot':
                                notice["preprint_embargo"] = "true"
                            else:
                                notice["preprint_embargo"] = None

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

                    if "modifiedDate_tdate" not in notice:
                        notice["modifiedDate_tdate"] = None
                    else:
                        notice["modifiedDate_tdate"] = datetime.fromisoformat(notice["modifiedDate_tdate"][:-1])
                    if "submittedDate_tdate" not in notice:
                        notice["submittedDate_tdate"] = None
                    else:
                        notice["submittedDate_tdate"] = datetime.fromisoformat(notice["submittedDate_tdate"][:-1])

                    res = es.index(index="half", id=notice["docid"], document=notice)
                    if res["_shards"]["successful"] == 0:
                        print("Error indexing")
                        print(notice)
                        print("\n")

    increment += rows

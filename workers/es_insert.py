import time
from datetime import datetime
import re
import os
import dateutil.parser
from dateutil.relativedelta import relativedelta
from fold_to_ascii import fold
from dateutil import parser

import requests
from nested_lookup import nested_lookup
from elasticsearch import Elasticsearch

from libs import qd

from names_dataset import NameDataset


nd = NameDataset()
# connexion à ES
es = Elasticsearch(hosts="http://elastic:" + os.environ.get('ES_PASSWORD') + "@localhost:9200/")

# flags nécessaires
flags = 'docid,halId_s,authIdHal_s,doiId_s,openAccess_bool,authIdHal_s,submittedDate_tdate,publicationDateY_i,modifiedDate_tdate,' \
        'title_s,*_abstract_s,*_keyword_s,fulltext_t,domain_s,primaryDomain_s,docType_s,labStructIdName_fs,labStructId_i,labStructAcronym_s,*_title_s,' \
        'conferenceEndDate_tdate,conferenceStartDate_tdate,defenseDate_tdate,ePublicationDate_tdate,owners_i,collCode_s,instStructId_i' \
        'producedDate_tdate,publicationDate_tdate,releasedDate_tdate,writingDate_tdate,instStructIdName_fs,linkExtUrl_s,linkExtId_s,' \
        'submittedDateY_i,submittedDateM_i,modifiedDateY_i,contributorId_i,contributorFullName_s,contributorFullNameId_fs,authFullName_s,structAddress_s,instStructAcronym_s,' \
        'authIdHasStructure_fs,structCode_s,structIdName_fs,structHasAlphaAuthId_fs,fileMain_s,authFullNameFormIDPersonIDIDHal_fs,selfArchiving_bool,' \
        'journalSherpaPrePrint_s,journalSherpaPostPrint_s,journalSherpaPostRest_s,journalSherpaPreRest_s,selfArchiving_bool,uri_s,journalTitle_s,journalTitleAbbr_s,journalId_i'

# paramètres HAL API
increment = 0
count = 1
rows = 10000


# gte = 2019
# lte = 2020
# to-do : >"2017-07-01T00:00:00Z"

gte = "2023-10-01T00:00:00Z"
lte = "2024-01-01T00:00:00Z"

# vérifie si le nom du contributeur est un nom de personne
def is_name(name):
    names_banlist = ["project", "migration", "imt", "service", "institutional", "repository", "bibliotheque", "archive",
                     "bibliothèque", "bmc", "elsevier", "institution", "institut", "archive", "migration", "irsn",
                     "images", "gestionnaire", "comité", "hal", "arxiv", "administrateur", "abes", "projet", "abes",
                     "import", "ifsttar", "centre", "documentation", "abes", "compte", "univ", " thèses",
                     "publications", "projet", "laboratoire", "institut", "université", "bu", "labo", "ecole", "médiathèque"]

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

# boucle dans les résultats de l'API
while increment < count:

    print(time.strftime("%H:%M:%S", time.localtime()), end=' : ')
    print(increment, end="/")
    print(count)

    res_status_ok = False
    while not res_status_ok:

        req = requests.get('https://api.archives-ouvertes.fr/search/?q=*&fl=' + flags + '&start=' + str(
            increment) + "&rows=" + str(rows) + "&fq=submittedDate_tdate:[" + str(gte) + " TO " + str(lte) +  "}&sort=docid%20asc")

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

                # pour chaque notice
                for notice in data['docs']:

                    # traitement du contributeur
                    if notice["selfArchiving_bool"] is True:
                        notice["contributor_type"] = "self"
                    else:
                        hal_error = False
                        if "authFullName_s" in notice:
                            for name in notice["authFullName_s"]:
                                if fold(name.lower()) in fold(notice["contributorFullName_s"].lower()):
                                    notice["contributor_type"] = "self"
                                    hal_error = True
                        if hal_error is False:

                            # process contributor name (or alias)
                            notice["contributor_type"] = None
                            name = notice["contributorFullNameId_fs"].split("_FacetSep_")[0]
                            is_name_bool = is_name(name)
                            if is_name_bool is True:
                                notice["contributor_type"] = "intermediate"
                            else:
                                notice["contributor_type"] = "automated"


                    # compte des auteurs
                    if "authFullName_s" in notice:
                        notice["count_authors"] = len(notice["authFullName_s"])
                    if "authIdHal_s" in notice:
                        notice["count_halIds"] = len(notice["authIdHal_s"])

                    if "count_authors" in notice and "count_halIds" in notice:
                        notice["authors_halIds_percentage"] = notice["count_halIds"] * 100 / notice["count_authors"]

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

                    # paramètres de QD individuels
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

                    # logique de dépôt
                    if notice["publicationDate_tdate"]:
                        deposit_delta = parser.parse(notice["submittedDate_tdate"]) - parser.parse(notice["publicationDate_tdate"])
                        if notice["has_file"] or notice["openAccess_bool"]:
                            # more than 1y
                            if deposit_delta.total_seconds() > 31536000:
                                notice["deposit_logic"] = "archiving"
                            elif deposit_delta.total_seconds() <= 31536000:
                                notice["deposit_logic"] = "communicating"
                        else:
                            # more than 1y
                            if deposit_delta.total_seconds() > 31536000:
                                notice["deposit_logic"] = "censusing"
                            elif deposit_delta.total_seconds() <= 31536000:
                                notice["deposit_logic"] = "referencing"

                    notice["deposit_delta"] = deposit_delta

                    if "modifiedDate_tdate" in notice:
                        notice["modified_delta"] = parser.parse(notice["modifiedDate_tdate"]) - parser.parse(notice["submittedDate_tdate"])

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
                    if "count_authors" not in notice:
                        notice["count_authors"] = None
                    if "count_halIds" not in notice:
                        notice["count_halIds"] = None

                    # filtres pour les éventuelles erreurs ES
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
                    if "modified_delta" not in notice:
                        notice["modified_delta"] = None
                    if "deposit_delta" not in notice:
                        notice["deposit_delta"] = None
                    if "authors_halIds_percentage" not in notice:
                        notice["authors_halIds_percentage"] = None
                    if 'contributorId_i' not in notice:
                        notice["contributorId_i"] = None
                    if 'deposit_logic' not in notice:
                        notice["deposit_logic"] = None
                    if 'doiId_s' not in notice:
                        notice["doiId_s"] = None
                    if "publicationDateY_i" not in notice:
                        notice["publicationDateY_i"] = None
                    if "publicationDate_tdate" not in notice:
                        notice["publicationDate_tdate"] = None
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
                    if "labStructId_i" not in notice:
                        notice["labStructId_i"] = None
                    if "labStructAcronym_s" not in notice:
                        notice["labStructAcronym_s"] = None
                    if "authIdHal_s" not in notice:
                        notice["authIdHal_s"] = None
                    if "authFullNameFormIDPersonIDIDHal_fs" not in notice:
                        notice["authFullNameFormIDPersonIDIDHal_fs"] = None
                    if "authFullName_s" not in notice:
                        notice["authFullName_s"] = None
                    if "linkExtId_s" not in notice:
                        notice["linkExtId_s"] = None
                    if "linkExtUrl_s" not in notice:
                        notice["linkExtUrl_s"] = None
                    if "modifiedDateY_i" not in notice:
                        notice["modifiedDateY_i"] = None
                    if "collCode_s" not in notice:
                        notice["collCode_s"] = None
                    if "journalTitle_s" not in notice:
                        notice["journalTitle_s"] = None
                    if "journalTitleAbbr_s" not in notice:
                        notice["journalTitleAbbr_s"] = None
                    if "journalId_i" not in notice:
                        notice["journalId_i"] = None
                    if "instStructId_i" not in notice:
                        notice["instStructId_i"] = None
                    if "instStructAcronym_s" not in notice:
                        notice["instStructAcronym_s"] = None

                    # traitement pour l'étude du vocabulaire (évolution SI/SIC)
                    if "fr_title_s" not in notice:
                        notice["fr_title_s"] = None
                    if "en_title_s" not in notice:
                        notice["en_title_s"] = None
                    if "fr_abstract_s" not in notice:
                        notice["fr_abstract_s"] = None
                    if "en_abstract_s" not in notice:
                        notice["en_abstract_s"] = None


                    notice_short = {
                        "docid": notice["docid"],

                        "halId_s": notice["halId_s"],
                        "docType_s": notice["docType_s"],
                        "uri_s": notice["uri_s"],

                        "fr_title_s": notice["fr_title_s"],
                        "en_title_s": notice["en_title_s"],

                        "fr_abstract_s": notice["fr_abstract_s"],
                        "en_abstract_s": notice["en_abstract_s"],

                        "collCode_s": notice["collCode_s"],
                        "journalId_i": notice["journalId_i"],
                        "journalTitle_s": notice["journalTitle_s"],
                        "journalTitleAbbr_s": notice["journalTitleAbbr_s"],

                        "labStructId_i": notice["labStructId_i"],

                        "authors_halIds_percentage" : notice["authors_halIds_percentage"],

                        "contributorFullNameId_fs": notice["contributorFullNameId_fs"],
                        "authFullNameFormIDPersonIDIDHal_fs": notice["authFullNameFormIDPersonIDIDHal_fs"],

                        "contributorFullName_s": notice["contributorFullName_s"],
                        "authFullName_s": notice["authFullName_s"],

                        "selfArchiving_bool": notice["selfArchiving_bool"],

                        "authIdHal_s": notice["authIdHal_s"],

                        "contributorId_i": notice["contributorId_i"],

                        "instStructIdName_fs": notice["instStructIdName_fs"],
                        "labStructIdName_fs": notice["labStructIdName_fs"],

                        "instStructAcronym_s": notice["instStructAcronym_s"],
                        "labStructAcronym_s": notice["labStructAcronym_s"],

                        "submittedDate_tdate": notice["submittedDate_tdate"],
                        "modifiedDate_tdate": notice["modifiedDate_tdate"],
                        "publicationDate_tdate": notice["publicationDate_tdate"],

                        "publicationDateY_i": notice["publicationDateY_i"],
                        "submittedDateY_i": notice["submittedDateY_i"],
                        "submittedDateM_i": notice["submittedDateM_i"],
                        "modifiedDateY_i": notice["modifiedDateY_i"],

                        "owners": len(notice["owners_i"]),

                        "qd": notice["qd"],

                        "deposit_logic": notice["deposit_logic"],

                        "preprint_embargo": notice["preprint_embargo"],
                        "postprint_embargo": notice["postprint_embargo"],

                        "contributor_type": notice["contributor_type"],

                        "domain_s": notice["domain_s"],
                        "primaryDomain_s": notice["primaryDomain_s"],

                        "doiId_s": notice["doiId_s"],

                        "openAccess_bool": notice["openAccess_bool"],

                        "has_file": notice["has_file"],
                        "has_abstract": notice["has_abstract"],
                        "has_keywords": notice["has_keywords"],

                        "linkExtId_s": notice["linkExtId_s"],
                        "linkExtUrl_s": notice["linkExtUrl_s"],

                        "deposit_delta": notice["deposit_delta"].total_seconds(),
                        "modified_delta": notice["modified_delta"].total_seconds(),

                        "count_authors": notice["count_authors"],
                        "count_halIds": notice["count_halIds"],

                        "harvested_on": datetime.now().replace(second=0, microsecond=0)
                    }

                    """
                    for key in notice_short.copy():
                        if notice_short[key] == "NULL":
                            del notice_short[key]
                    """

                    # if document exists, update it
                    # q = {
                    #     "query": {
                    #         "match": {
                    #                   "docid": notice["docid"]
                    #               }
                    #     }
                    # }
                    # upd_count = es.count(index="hal4", body=q)["count"]
                    # if upd_count > 0:
                    #     es.update(index="hal4", id=notice["docid"], body={"doc": notice_short})
                    # else:
                    res = es.index(index="hal-2023", id=notice["docid"], document=notice_short)
                    if res["_shards"]["successful"] == 0:
                        print("Error indexing")
                        print(notice_short)
                        print("\n")
    increment += rows

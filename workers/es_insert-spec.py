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
es = Elasticsearch(hosts="http://elastic:" + os.environ.get('ES_PASSWORD') + "@localhost:9200/")

flags = 'docid,halId_s,authIdHal_s,doiId_s,openAccess_bool,authIdHal_s,submittedDate_tdate,publicationDateY_i,modifiedDate_tdate,' \
        'title_s,*_abstract_s,*_keyword_s,fulltext_t,domain_s,primaryDomain_s,docType_s,labStructIdName_fs,' \
        'conferenceEndDate_tdate,conferenceStartDate_tdate,defenseDate_tdate,ePublicationDate_tdate,' \
        'producedDate_tdate,publicationDate_tdate,releasedDate_tdate,writingDate_tdate,instStructIdName_fs,' \
        'submittedDateY_i,submittedDateM_i,modifiedDateY_i,contributorId_i,contributorFullName_s,contributorFullNameId_fs,authFullName_s,structAddress_s,' \
        'authIdHasStructure_fs,structCode_s,structIdName_fs,structHasAlphaAuthId_fs,fileMain_s,' \
        'journalSherpaPrePrint_s,journalSherpaPostPrint_s,journalSherpaPostRest_s,journalSherpaPreRest_s,selfArchiving_bool,uri_s'

increment = 0
count = 1
rows = 10000


# gte = 2019
# lte = 2020
# to-do : >"2017-07-01T00:00:00Z"

gte = "2022-11-01T00:00:00Z"
lte = "2022-12-01T00:00:00Z"

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

docids = ['1858401', '3875159', '3884398', '3070259', '3091649', '3853484', '3885019', '3577283', '3582919', '3582945', '3583038', '3583054', '3583067', '3583079', '3583097', '3599509', '3599512', '3601457', '3620103', '3834272', '3886081', '3659481', '3659544', '3659710', '3659722', '3714109', '3714295', '3715333', '3716337', '3723821', '3750198', '3753656', '3763592', '3766042', '3885630', '2472603', '3082426', '3082488', '3082511', '3277817', '3450039', '3450047', '3450100', '3454405', '3467459', '3480409', '3499752', '3509906', '3605970', '3617846', '3634442', '3654555', '3714860', '3760527', '3770180', '3775490', '3776047', '3776192', '3776234', '3783393', '3791857', '3792698', '3793135', '3793176', '3793217', '3793776', '3796766', '3797613', '3798508', '3800334', '3805880', '3805892', '3806407', '3806672', '3807255', '3807261', '3807321', '3807324', '3807594', '3807623', '3807634', '3808462', '3810041', '3810299', '3810342', '3810497', '3811502', '3811647', '3811822', '3812104', '3812118', '3812407', '3812724', '3813033', '3813035', '3813038', '3813115', '3813117', '3813121', '3813127', '3813135', '3813758', '3813796', '3813891', '3813993', '3814206', '3814396', '3814397', '3814528', '3814534', '3814535', '3814537', '3814698', '3815171', '3815498', '3816042', '3816317', '3816782', '3816790', '3817086', '3817126', '3817992', '3818018', '3818588', '3819837', '3820424', '3820734', '3821248', '3822797', '3823134', '3823253', '3823397', '3823399', '3823402', '3823918', '3824980', '3826392', '3826969', '3827028', '3827123', '3827330', '3827630', '3827650', '3827656', '3827658', '3827666', '3827673', '3827683', '3827689', '3827726', '3828216', '3828317', '3828412', '3828608', '3829686', '3831097', '3831766', '3831779', '3831955', '3831997', '3832210', '3832243', '3832833', '3833699', '3834065', '3834393', '3835073', '3835664', '3835751', '3835883', '3835890', '3835999', '3836122', '3836335', '3836368', '3836393', '3836407', '3836445', '3836483', '3836558', '3836773', '3836783', '3836789', '3836832', '3836935', '3837232', '3837398', '3837491', '3837533', '3837545', '3837551', '3837914', '3837949', '3838099', '3838416', '3838864', '3838958', '3839170', '3839313', '3839542', '3839581', '3839598', '3839687', '3839708', '3839719', '3839751', '3839775', '3839785', '3839790', '3839799', '3839821', '3839845', '3840144', '3840331', '3840535', '3840536', '3840553', '3840556', '3840560', '3840566', '3840570', '3840592', '3840596', '3840598', '3840600', '3840604', '3840605', '3840968', '3841226', '3841412', '3841603', '3841689', '3841862', '3843193', '3843284', '3843548', '3843832', '3844127', '3844544', '3844824', '3845216', '3845307', '3845435', '3845625', '3845704', '3845864', '3845985', '3846031', '3846070', '3846096', '3846098', '3846684', '3846714', '3846782', '3847157', '3847340', '3847542', '3848256', '3848644', '3849361', '3849585', '3849635', '3849822', '3849909', '3850137', '3850644', '3850733', '3850806', '3850810', '3850815', '3851118', '3851317', '3851625', '3851778', '3851815', '3851838', '3851930', '3852498', '3852763', '3853407', '3853429', '3853528', '3854104', '3854292', '3854646', '3854666', '3854668', '3854687', '3854735', '3854827', '3855356', '3855518', '3855529', '3855805', '3855983', '3856080', '3856184', '3856216', '3856262', '3856287', '3856294', '3856301', '3856307', '3856320', '3856322', '3856336', '3856340', '3856621', '3856648', '3856668', '3856697', '3856702', '3856802', '3856821', '3856962', '3857147', '3857302', '3857354', '3857549', '3857551', '3857594', '3857627', '3857662', '3857664', '3857710', '3857720', '3857766', '3857767', '3857807', '3857820', '3857845', '3857874', '3857925', '3857959', '3857968', '3857983', '3858013', '3858044', '3858045', '3858166', '3858266', '3858300', '3858341', '3858398', '3858419', '3858520', '3858523', '3858639', '3858731', '3858817', '3858818', '3858819', '3858970', '3858973', '3858974', '3859100', '3859199', '3859311', '3859408', '3859428', '3859433', '3859460', '3859716', '3859722', '3860079', '3860181', '3860187', '3860194', '3860207', '3860402', '3860420', '3860427', '3860476', '3860760', '3860827', '3860830', '3860838', '3860839', '3860852', '3860862', '3860875', '3860878', '3860881', '3860884', '3860896', '3860899', '3861007', '3861274', '3861582', '3861584', '3861608', '3861633', '3861859', '3862023', '3862085', '3862144', '3862158', '3862196', '3862225', '3862266', '3862324', '3862590', '3862753', '3862988', '3863235', '3863391', '3863598', '3863605', '3863808', '3863818', '3863868', '3863915', '3863921', '3863926', '3863932', '3863935', '3863936', '3863938', '3863991', '3864139', '3864183', '3864217', '3864248', '3864281', '3864303', '3864322', '3864606', '3864610', '3864641', '3864727', '3865387', '3865434', '3865921', '3865983', '3866002', '3866052', '3866305', '3866449', '3866732', '3866736', '3866745', '3866860', '3867023', '3867064', '3867157', '3867182', '3867244', '3867267', '3867396', '3867460', '3867514', '3867768', '3867858', '3867892', '3868032', '3868271', '3868305', '3868327', '3868361', '3868372', '3868436', '3868438', '3868542', '3868749', '3868781', '3868785', '3868825', '3868835', '3868865', '3868869', '3868880', '3868890', '3868920', '3868958', '3868960', '3868979', '3868991', '3869003', '3869007', '3869027', '3869031', '3869051', '3869053', '3869058', '3869065', '3869070', '3869105', '3869128', '3869130', '3869144', '3869151', '3869152', '3869161', '3869164', '3869199', '3869200', '3869205', '3869231', '3869241', '3869248', '3869256', '3869260', '3869292', '3869326', '3869398', '3869440', '3869454', '3869467', '3869483', '3869486', '3869487', '3869547', '3869566', '3869639', '3869654', '3869691', '3869693', '3869699', '3869715', '3869730', '3869734', '3869824', '3869837', '3869864', '3869874', '3869891', '3869964', '3869989', '3869993', '3870015', '3870016', '3870081', '3870110', '3870141', '3870145', '3870160', '3870173', '3870233', '3870281', '3870370', '3870400', '3870492', '3870559', '3870563', '3870564', '3870569', '3870571', '3870575', '3870576', '3870587', '3870589', '3870597', '3870602', '3870605', '3870615', '3870617', '3870618', '3870627', '3870634', '3870642', '3870645', '3870684', '3870700', '3870721', '3870741', '3870789', '3870833', '3870886', '3870915', '3870938', '3870961', '3870973', '3870982', '3870992', '3870995', '3871003', '3871005', '3871035', '3871077', '3871089', '3871094', '3871098', '3871118', '3871119', '3871132', '3871145', '3871184', '3871188', '3871194', '3871222', '3871250', '3871261', '3871286', '3871297', '3871301', '3871313', '3871314', '3871324', '3871347', '3871367', '3871390', '3871394', '3871431', '3871449', '3871458', '3871477', '3871494', '3871495', '3871523', '3871528', '3871536', '3871564', '3871571', '3871578', '3871583', '3871586', '3871591', '3871594', '3871610', '3871640', '3871658', '3871666', '3871670', '3871677', '3871678', '3871684', '3871685', '3871727', '3871737', '3871744', '3871748', '3871754', '3871760', '3871761', '3871766', '3871768', '3871769', '3871773', '3871783', '3871784', '3871803', '3871811', '3871832', '3871840', '3871879', '3871880', '3871887', '3871888', '3871889', '3871895', '3871896', '3871900', '3871901', '3871904', '3871905', '3871907', '3871915', '3871917', '3871919', '3871924', '3871926', '3871929', '3871945', '3871949', '3871950', '3871951', '3871963', '3871979', '3871984', '3871999', '3872011', '3872016', '3872036', '3872038', '3872044', '3872060', '3872075', '3872078', '3872087', '3872104', '3872109', '3872124', '3872142', '3872180', '3872229', '3872249', '3872262', '3872267', '3872269', '3872290', '3872298', '3872302', '3872304', '3872309', '3872317', '3872328', '3872337', '3872346', '3872361', '3872370', '3872374', '3872388', '3872395', '3872399', '3872407', '3872408', '3872411', '3872422', '3872427', '3872445', '3872446', '3872448', '3872451', '3872456', '3872466', '3872479', '3872483', '3872499', '3872505', '3872518', '3872530', '3872536', '3872540', '3872547', '3872549', '3872631', '3872635', '3872639', '3872645', '3872655', '3872691', '3872732', '3872743', '3872746', '3872752', '3872769', '3872771', '3872775', '3872777', '3872781', '3872782', '3872784', '3872790', '3872792', '3872795', '3872796', '3872818', '3872837', '3872842', '3872865', '3872867', '3872873', '3872881', '3872907', '3872909', '3872910', '3872912', '3872917', '3872964', '3872965', '3872966', '3872976', '3872983', '3872985', '3872994', '3872997', '3873014', '3873015', '3873024', '3873027', '3873031', '3873035', '3873042', '3873559', '3873650', '3873654', '3873706', '3873710', '3873712', '3873721', '3873725', '3873736', '3873741', '3873744', '3873748', '3873756', '3873758', '3873762', '3873765', '3873773', '3873785', '3873791', '3873800', '3873801', '3873802', '3873803', '3873805', '3873809', '3873824', '3873833', '3873878', '3873897', '3873903', '3873908', '3873913', '3873919', '3873926', '3873930', '3873937', '3873999', '3874007', '3874010', '3874017', '3874027', '3874031', '3874153', '3874165', '3874254', '3874257', '3874261', '3874545', '3874567', '3874732', '3874782', '3875065', '3875203', '3875225', '3875361', '3875447', '3875914', '3875924', '3875925', '3875926', '3875928', '3875935', '3875993', '3876005', '3876069', '3876093', '3876114', '3876140', '3876142', '3876147', '3876181', '3876199', '3876218', '3876287', '3876318', '3876351', '3876448', '3876455', '3876476', '3876483', '3876535', '3876599', '3876605', '3876634', '3876638', '3876641', '3876644', '3876722', '3876830', '3876833', '3876839', '3876850', '3876852', '3876855', '3876881', '3876884', '3876885', '3876939', '3876990', '3876993', '3877007', '3877008', '3877014', '3877018', '3877034', '3877041', '3877069', '3877076', '3877155', '3877170', '3877177', '3877227', '3877249', '3877257', '3877282', '3877344', '3877345', '3877378', '3877390', '3877397', '3877398', '3877416', '3877419', '3877425', '3877431', '3877432', '3877452', '3877486', '3877500', '3877518', '3877551', '3877552', '3877555', '3877576', '3877606', '3877628', '3877661', '3877672', '3877688', '3877694', '3877815', '3877871', '3877907', '3877943', '3877986', '3877993', '3878062', '3878119', '3878160', '3878175', '3878192', '3878231', '3878238', '3878290', '3878351', '3878354', '3878378', '3878392', '3878406', '3878408', '3878412', '3878634', '3878666', '3878667', '3878669', '3878678', '3878686', '3878694', '3878697', '3878700', '3878704', '3878711', '3878722', '3878723', '3878728', '3878729', '3878781', '3878785', '3878863', '3878871', '3878872', '3878901', '3878967', '3878968', '3878990', '3879045', '3879071', '3879197', '3879211', '3879225', '3879240', '3879320', '3879354', '3879363', '3879409', '3879433', '3879446', '3879491', '3879572', '3879608', '3879619', '3879704', '3879737', '3879807', '3879901', '3879921', '3879994', '3880064', '3880070', '3880088', '3880125', '3880132', '3880142', '3880143', '3880145', '3880146', '3880147', '3885454', '3885588', '3885602']

for docid in docids:
    req = requests.get('https://api.archives-ouvertes.fr/search/?q=docid:' + docid + '&fl=' + flags + "&sort=docid%20asc")

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
                    # notice["modifiedDateY_i"] = notice["submittedDateY_i"]
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
                                notice["contributor_type"] = "self"
                                notice["contributor_type_processed"] = "self"
                                hal_error = True
                    if hal_error is False:
                            notice["contributor_type"] = "other_FacetSep_" + str(notice["contributorFullNameId_fs"])

                            # process contributor name (or alias)
                            notice["contributor_type_processed"] = None
                            name = notice["contributor_type"].split("_FacetSep_")[1]
                            contributor_type_processed = is_name(name)
                            if contributor_type_processed is True:
                                notice["contributor_type_processed"] = "intermediate"
                            else:
                                notice["contributor_type_processed"] = "automated"


                # count authors
                if "authFullName_s" in notice:
                    notice["count_authors"] = len(notice["authFullName_s"])

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

                # deposit logic
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
                if "authIdHal_s" not in notice:
                    notice["authIdHal_s"] = None
                if "nb_authors" not in notice:
                    notice["nb_authors"] = None

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
                    "uri_s": notice["uri_s"],

                    "authIdHal_s": notice["authIdHal_s"],

                    "nb_authors": notice["nb_authors"],

                    "instStructIdName_fs": notice["instStructIdName_fs"],
                    "labStructIdName_fs": notice["labStructIdName_fs"],

                    "inst_name": notice["inst_name"],
                    "lab_name": notice["lab_name"],

                    "submittedDate_tdate": notice["submittedDate_tdate"],
                    "modifiedDate_tdate": notice["modifiedDate_tdate"],
                    "publicationDate_tdate": notice["publicationDate_tdate"],

                    "publicationDateY_i": notice["publicationDateY_i"],
                    "submittedDateY_i": notice["submittedDateY_i"],
                    "submittedDateM_i": notice["submittedDateM_i"],
                    "modifiedDateY_i": notice["modifiedDateY_i"],

                    "qd": notice["qd"],

                    "deposit_logic": notice["deposit_logic"],

                    "preprint_embargo": notice["preprint_embargo"],
                    "postprint_embargo": notice["postprint_embargo"],

                    "contributor_type": notice["contributor_type"],
                    "contributor_type_processed": notice["contributor_type_processed"],

                    "domain_s": notice["domain_s"],
                    "primaryDomain_s": notice["primaryDomain_s"],

                    "doiId_s": notice["doiId_s"],

                    "openAccess_bool": notice["openAccess_bool"],

                    "has_file": notice["has_file"],
                    "has_abstract": notice["has_abstract"],
                    "has_keywords": notice["has_keywords"],

                    "harvested_on": datetime.now().replace(second=0, microsecond=0)
                }

                """
                for key in notice_short.copy():
                    if notice_short[key] == "NULL":
                        del notice_short[key]
                """

                # if document exists, update it
                q = {
                    "query": {
                        "match": {
                                  "docid": notice["docid"]
                              }
                    }
                }

                upd_count = es.count(index="hal2", body=q)["count"]
                if upd_count > 0:
                    es.update(index="hal2", id=notice["docid"], body={"doc": notice_short})
                else:
                    res = es.index(index="hal2", id=notice["docid"], document=notice_short)
                    if res["_shards"]["successful"] == 0:
                        print("Error indexing")
                        print(notice_short)
                        print("\n")

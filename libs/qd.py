from nested_lookup import nested_lookup


def calculate(notice):
    score = 0

    abstract_penalty = False

    if 'title_s' in notice:
        has_title = True
        if notice['title_s'][0] == "":
            has_title = False
    else:
        has_title = False

    if 'doiId_s' in notice:
        has_doi = True
        if notice['doiId_s'] == "":
            has_doi = False
    else:
        has_doi = False

    if 'publicationDate_tdate' in notice or 'conferenceStartDate_tdate' in notice or 'conferenceEndDate_tdate' in notice \
            or 'defenseDate_tdate' in notice or 'ePublicationDate_tdate' in notice or 'producedDate_tdate' in notice \
            or 'releasedDate_tdate' in notice or 'writingDate_tdate':
        has_publication_date = True
    else:
        has_publication_date = False

    if 'domain_s' in notice:
        has_domain = True
    else:
        has_domain = False

    keywords = nested_lookup(
        key="_keyword_s",
        document=notice,
        wild=True,
        with_keys=True,
    )

    if len(keywords) > 0:
        has_kw = True
    else:
        has_kw = False

    abstracts = nested_lookup(
        key="_abstract_s",
        document=notice,
        wild=True,
        with_keys=True,
    )

    sub_abstract_penalty = 0
    for abstract in abstracts:

        title_words_count = len(notice["title_s"][0].split())

        if (len(notice[abstract][0].split()) < 3) or (len(notice[abstract][0].split()) < title_words_count):
            sub_abstract_penalty += 1

    if sub_abstract_penalty == len(abstracts) and (sub_abstract_penalty != 0):
        abstract_penalty = True

    if len(abstracts) > 0 and not abstract_penalty:
        has_abstract = True
    else:
        has_abstract = False

    if 'fileMain_s' in notice or "linkExtUrl_s" in notice or notice["openAccess_bool"] == 1:
        has_attached_file = True
    else:
        has_attached_file = False

    if has_title:
        score += 1 * 0.1
    if has_publication_date:
        score += 1 * 0.1
    if has_kw:
        score += 1 * 1
    if has_abstract:
        score += 1 * 0.8
    if has_attached_file:
        score += 1 * 0.4
    if has_doi:
        score += 1 * 0.6
    if has_domain:
        score += 1 * 0.1

    return score / (0.1 + 0.1 + 1 + 0.8 + 0.4 + 0.6 + 0.1)

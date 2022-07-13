import requests


def get_metrics(doiId_s):
    res = {}
    response = requests.request("GET", "https://metrics-api.dimensions.ai/doi/" + doiId_s)
    if response.status_code == 200:
        response = response.json()
        if "times_cited" in response:
            res['times_cited'] = response['times_cited']
            res['field_citation_ratio'] = response['field_citation_ratio']
    return res

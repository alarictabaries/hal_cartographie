import os
import requests
from elasticsearch import Elasticsearch

import numpy as np


def lolvelty(es, index, doc_id, fields,
             similar_perc=25,
             max_query_terms=25,
             max_doc_frac=0.75,
             minimum_should_match=0.3,
             human_friendly=True,
             total=None):
    """Simple statistical measurement of novelty. First,
    the 1000 most similar documents are retrieved
    from elasticsearch, scored by tfidf and normalised to
    the document itself. Novelty is then defined as
    (1 - quantile), where the quantile is defined by the user.
    "Novel" documents are intuitively very seperated from lower
    quantile (i.e. unrelated) documents, and so using a quantile
    of e.g. 25% leads to intuitive results.
    The score is optionally, rescaled to have a human-friendly
    scale, since humans don't deal well with fractions.
    Args:
        es (elasticsearch.Elasticsearch): Elasticsearch object.
        index (str): Elasticsearch index to query.
        doc_id (str): Document id in Elasticsearch to rank.
        fields (list): List of fields to determine novelty from.
        max_query_terms (int): Maximum number of terms to determine
                               similarity from.
        max_doc_frac (float): Maximum fraction of documents a term can
                              be present in (cuts out stop words).
        minimum_should_match (float): Minimum number of query terms that
                                      should be present in all documents.
        total (int): Total count of documents in the index. Pass this in
                     to save a little processing time.
    Returns:
        score (float): A novelty score.
    """
    # Calculate total if required
    if total is None:
        r = es.count(index=index,
                     body={"query": {"match_all": {}}})
        total = r['count']
    # Build mlt query
    max_doc_freq = int(max_doc_frac * total)
    minimum_should_match = f"{int(minimum_should_match * 100)}%"
    mlt_query = {
        "query": {
            "more_like_this": {
                "fields": fields,
                "like": [{'_id': doc_id,
                          '_index': index}],
                "min_term_freq": 1,
                "max_query_terms": max_query_terms,
                "min_doc_freq": 1,
                "max_doc_freq": max_doc_freq,
                "boost_terms": 1.,
                "minimum_should_match": minimum_should_match,
                "include": True
            }
        },
        "size": 1000,
        "_source": ["_score"]
    }
    # Make the search and normalise the scores
    r = es.search(index=index, body=mlt_query)
    scores = [h['_score'] / r['hits']['max_score']
              for h in r['hits']['hits']]
    if len(scores) <= 1:
        return None
    # Calculate the novelty
    delta = np.percentile(scores, similar_perc)
    if human_friendly:
        return 10 * (similar_perc - 100 * delta)
    return 1 - delta


es = Elasticsearch(hosts="http://elastic:" + os.environ.get('ES_PASSWORD') + "@localhost:9200/")

search_request = "chimie click"
req = requests.get('https://api.archives-ouvertes.fr/search/?q=fulltext_t:' + search_request + '*&fl=docid,title_s')

search_request = "kouamvi-couao-zotti"
req = requests.get('https://api.archives-ouvertes.fr/search/?q=authIdHal_s:' + search_request + '&fl=docid,title_s')

results = []

if req.status_code == 200:
    print(req)
    data = req.json()
    for notice in data["response"]['docs']:
        print(notice["title_s"])
        results.append({"_id": notice['docid'], "_index": "hal-test"})

mlt_query = {"query":
                 {"more_like_this":
                      {"fields": ["fr_abstract_s", "fr_keywords_s", "fr_title_s"],
                       "like": results,
                       "boost_terms": 1.,
                       "include": False
                       }
                  }
             }

r = es.search(index="hal-test", body=mlt_query)
for hit in r['hits']['hits']:
    print(hit['_source']['fr_title_s'], end=" - ")
    print("https://hal.archives-ouvertes.fr/" + hit['_source']['halId_s'])

keyword = "hemostatic"

kw_query = {"query": {"match": {"en_keyword_s": keyword}},
            "size": 0,
            "aggregations": {
                "my_sample": {
                    "sampler": {"shard_size": 10000},
                    "aggregations": {
                        "keywords": {
                            "significant_text": {
                                "size": 10,
                                "field": "fr_keyword_s",
                                "gnd": {}
                            }
                        }
                    }
                }
            }
            }

r = es.search(index="hal-test", body=kw_query)
for hit in r['aggregations']['my_sample']['keywords']['buckets']:
    print(hit['key'])

# q = lolvelty(es, "hal-test", "878994", ["fr_abstract_s", "fr_keywords_s", "fr_title_s"])

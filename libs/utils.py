from SPARQLWrapper import SPARQLWrapper, JSON
from names_dataset import NameDataset

nd = NameDataset()


def is_a_name(s):
    name_banlist = ["project", "imt", "service", "institutional", "repository", "bibliotheque", "archive", "bibliothèque", "bmc", "elsevier", "institution", "institut", "archive", "migration", "irsn", "images",
                    "gestionnaire", "comité", "hal", "arxiv", "administrateur", "abes", "projet", "abes", "import",
                    "ifsttar", "centre", "documentation", "abes", "compte", "univ"," thèses", "publications", "projet"]
    doubt = False

    name_whitelist = ["anavaj", " bulgantsengel", " gwennoline", "tibault", "jean-raphaël", "lenaïc", "malyk", "rené-jean", "jaonary", "martintxo", "tudor-bogdan", "peillot"]

    for word in s.split(" "):
        if word.lower() in name_banlist:
            return False
    for word in s.split(" "):
        if len(word) > 1:
            if word[1] == "." and len(s) == 2:
                doubt = True
        result = nd.search(word)
        if result["first_name"] != None or result["last_name"] != None:
            return True
        else:
            if word.lower() in name_whitelist:
                return True
    if doubt:
        return True
    else:
        return False

# not used anymore
def calculate_distance(s, t):
    diff = 0

    if len(t.split(" ")) > 3:
        return 0

    if len(s) > len(t):
        for i in t:
            if i not in s:
                diff += 1
        pct = (len(t) - diff) * 100 / len(t)
    else:
        for i in s:
            if i not in t:
                diff += 1
        pct = (len(s) - diff) * 100 / len(s)

    return pct / 100


def getAddress(structId):
    sparql = SPARQLWrapper("http://sparql.archives-ouvertes.fr/sparql")
    sparql.setReturnFormat(JSON)

    try:
        sparql.setQuery("""
            select ?p ?o
            where  {
            <https://data.archives-ouvertes.fr/structure/%s> ?p ?o
            }""" % structId)
        results = sparql.query().convert()

        address = [truc for truc in results['results']['bindings'] if
                   truc['p']['value'] == "http://www.w3.org/ns/org#siteAddress"]

        return address[0]['o']['value']
    except:
        return None

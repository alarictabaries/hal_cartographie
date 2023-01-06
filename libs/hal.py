import requests
import time
from bs4 import BeautifulSoup
import html

from requests.adapters import HTTPAdapter
from urllib3.util import Retry

retry_strategy = Retry(
    total=2,
    backoff_factor=0.2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "DELETE", "PUT", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)


def count_notices(field, value):
    res_status_ok = False
    while not res_status_ok:
        # req = http.get('https://api.archives-ouvertes.fr/search/?q=' + field + ':' + str(value))
        req = requests.request("GET", 'https://api.archives-ouvertes.fr/search/?q=' + field + ':' + str(value))
        if req.status_code == 200:
            data = req.json()
            if "error" in data.keys():
                print("Error: ", end=":")
                print(data["error"])
                time.sleep(60)
            if "response" in data.keys():
                return data["response"]["numFound"]


def get_metrics(uri_s):
    res = {}
    res_ok = False
    res_retries = 0


    while res_ok is False and res_retries < 4:
        # notice = http.get(uri_s, verify=False)
        try:
            notice = requests.request("GET", uri_s, verify=False)
            notice_t = html.unescape(notice.text)
            soup = BeautifulSoup(notice_t, 'html.parser')
            try:
                metrics = soup.find_all(class_='widget-metrics')[0].find(class_="row").findChildren(recursive=False)
                for metric in metrics:
                    if "Consultations de la notice" in metric.text or "Record views" in metric.text:
                        res['times_viewed'] = int(metric.find_all(class_="label-primary")[0].text)
                        res_ok = True
                    if "Téléchargements de fichiers" in metric.text or "Files downloads" in metric.text:
                        res['times_downloaded'] = int(metric.find_all(class_="label-primary")[0].text)
            except:
                try:
                    # URI with version number (ex: https://hal.archives-ouvertes.fr/ijn_02985466v2)
                    if not uri_s.split("-")[-1].isdigit() or len(uri_s.split("-")[-1]) != 8:
                        uri_s = "https://hal.archives-ouvertes.fr/" + uri_s.split("/")[-1].split("v")[0]
                    # hceres has no stats
                    elif "hal-hceres.archives-ouvertes.fr" in uri_s:
                        return res
                    elif all(s in soup.find_all(class_='jumbotron')[0].text for s in ["Le document n'a pas été trouvé", "n'existe pas"]):
                        if uri_s.split("/")[-1][-2] == "v":
                            uri_s = uri_s[:-2]
                        else:
                            res["deleted_notice"] = True
                            return res
                    # wrong URI
                    elif "Le document n'est pas visible dans cet espace." in soup.find_all(class_='jumbotron')[0].text:
                        uri_s = "https://hal.archives-ouvertes.fr/" + uri_s.split("/")[-1]
                        if res_retries > 1:
                            uri_s = "https://hal.archives-ouvertes.fr/view/resolver?identifiant=" + uri_s.split("/")[-1]
                    elif "Le document n'est pas indexé" in soup.find_all(class_='jumbotron')[0].text:
                        return res
                    else:
                        time.sleep(0.3)
                except Exception as e:
                    print(uri_s, end=" :")
                    print(e)
                    # no metrics on hal-hceres portal
                    if "hal-hceres.archives-ouvertes.fr" in uri_s:
                        res_retries = 4
                    pass
        except Exception as e:
            if res_retries > 2:
                print(uri_s, end=" :")
                print(e)
            if "Exceeded 30 redirects" in str(e):
                uri_s = "https://hal.archives-ouvertes.fr/" + uri_s.split("/")[-1]
            # bad uri (too_many_redirects)
            uri_s = "https://hal.archives-ouvertes.fr/" + uri_s.split("/")[-1]
        res_retries += 1

    if res_ok is False:
        print(uri_s, end = " : ")
        print("error retrieving metrics")
    return res

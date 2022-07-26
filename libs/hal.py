import requests
import time
from bs4 import BeautifulSoup

from requests.adapters import HTTPAdapter
from urllib3.util import Retry

retry_strategy = Retry(
    total=4,
    backoff_factor=1,
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
        req = http.get('https://api.archives-ouvertes.fr/search/?q=' + field + ':' + str(value))
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
        notice = http.get(uri_s, verify=False)
        soup = BeautifulSoup(notice.text, 'html.parser')
        try:
            metrics = soup.find_all(class_='widget-metrics')[0].find(class_="row").findChildren(recursive=False)
            for metric in metrics:
                if "Consultations de la notice" in metric.text:
                    res['times_viewed'] = int(metric.find_all(class_="label-primary")[0].text)
                    res_ok = True
                if "Téléchargements de fichiers" in metric.text:
                    res['times_downloaded'] = int(metric.find_all(class_="label-primary")[0].text)
        except:
            if "Le document n'est pas visible dans cet espace." in soup.find_all(class_='jumbotron')[0].text:
                uri_s = "https://hal.archives-ouvertes.fr/" + uri_s.split("/")[-1]
            else:
                time.sleep(1)
        res_retries += 1

    if res_ok is False:
        print(uri_s)
        print("error retrieving metrics")

    return res

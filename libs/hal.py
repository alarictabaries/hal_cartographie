import pandas as pd
import requests
import time
from bs4 import BeautifulSoup


def get_metrics(uri_s):

    res = {}

    notice = requests.get(uri_s)
    try:
        soup = BeautifulSoup(notice.text, 'html.parser')
        metrics = soup.find_all(class_='widget-metrics')[0].find(class_="row").findChildren(recursive=False)
        for metric in metrics:
            if "Consultations de la notice" in metric.text:
                res['times_viewed'] = metric.find_all(class_="label-primary")[0].text
            if "Téléchargements de fichiers" in metric.text:
                res['times_downloaded'] = metric.find_all(class_="label-primary")[0].text
    except:
        print("can not retrieve HAL document")
        print(uri_s)


    return res
import pandas as pd
import requests
import time
from bs4 import BeautifulSoup


def get_metrics(halId_s):

    res = {}
    notice = requests.get("https://hal.archives-ouvertes.fr/" + halId_s)
    soup = BeautifulSoup(notice.text, 'html.parser')
    try:
        metrics = soup.find_all(class_='widget-metrics')[0].find(class_="row").findChildren(recursive=False)
        for metric in metrics:
            if "Consultations de la notice" in metric.text:
                res['times_viewed'] = metric.find_all(class_="label-primary")[0].text
            if "Téléchargements de fichiers" in metric.text:
                res['times_downloaded'] = metric.find_all(class_="label-primary")[0].text
    except Exception as e:
        # trick to avoid the error when a document is on another portal
        try:
            new_link = "https://hal.archives-ouvertes.fr" + \
                       soup.find_all(class_='jumbotron')[0].find_all('a', href=True)[0]['href']
            notice = requests.get(new_link)
            soup = BeautifulSoup(notice.text, 'html.parser')
            metrics = soup.find_all(class_='widget-metrics')[0].find(class_="row").findChildren(recursive=False)
            for metric in metrics:
                if "Consultations de la notice" in metric.text:
                    res['times_viewed'] = metric.find_all(class_="label-primary")[0].text
                if "Téléchargements de fichiers" in metric.text:
                    res['times_downloaded'] = metric.find_all(class_="label-primary")[0].text
        except Exception as e:
            pass

    return res
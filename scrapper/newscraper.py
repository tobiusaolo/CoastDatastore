from bs4 import BeautifulSoup, SoupStrainer, NavigableString, Tag
from sqlalchemy import create_engine
import time
import csv
import requests
import re
import os
import numpy as np
import urllib.request
import urllib
import html5lib
import pandas as pd
import nltk
import datetime
from nltk.tokenize.punkt import PunktSentenceTokenizer, PunktParameters
from pandas import DataFrame
import dropbox
dropbox_access_token = "-EBxOhuzOI4AAAAAAAAAAU1JAwmpo6Xy9PsYOguc1-mFh5QJsWAvENXusH06oWXr"
client = dropbox.Dropbox(dropbox_access_token)
db_connect = create_engine(
    'postgresql://awjzgmwqiatzjg:e4424ae3d375e2057bcc9cde832672940d44ea2c05260e28ccb04dc1575ec52d@ec2-34-204-22-76.compute-1.amazonaws.com:5432/dabbhqt4pegslv')
conn = db_connect.connect()
timez = datetime.datetime.today().strftime("%Y/%m/%d")
time = datetime.datetime.today().strftime("%Y-%m-%d")


def gambuuze_scrapper():
    general_name = '_gambuuze.csv'
    filename = time+general_name
    dropbox_path = "/Coast_data/"+filename
    datasource = "Gambuuze news"
    scraped_data = []
    today_articles = []
    heads_lines = []
    url = url = "https://gambuuze.ug/"+timez+"/"
    r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    soup = BeautifulSoup(r.content)
    snippet = soup.find_all('h3', attrs={"class": "jeg_post_title"})
    for i in snippet:
        for link in i.find_all('a'):
            line = link.get('href')
            today_articles.append(line)
    today_articles = list(set(today_articles))
    for x in today_articles:
        if str(timez) in x:
            r = requests.get(x, headers={'User-Agent': 'Mozilla/5.0'})
            soup = BeautifulSoup(r.content)
            for division in soup.find_all("div", attrs={"class": "content-inner"}):
                for link in division.find_all('p'):
                    f = link.get_text()
                    sentences = nltk.sent_tokenize(f)
                    for sentence in sentences:
                        scraped_data.append(sentence)
            snippet = soup.find_all(
                'div', attrs={"class": "jeg_inner_content"})
            for i in snippet:
                for link in i.find_all('h1'):
                    fl = link.get_text()
                    hsentences = nltk.sent_tokenize(fl)
                    for hsentence in hsentences:
                        heads_lines.append(hsentence)
    scraped_data.extend(heads_lines)
    df = pd.DataFrame(scraped_data, columns=['Luganda'])
    df.drop_duplicates(subset='Luganda', keep='first', inplace=True)
    df['Luganda'].replace('  ', np.nan, inplace=True)
    corpus_length = len(df)
    df = df.dropna(subset=['Luganda'])
    if corpus_length >= 2:
        df.to_csv(filename, index=False)
        client.files_upload(open(filename, "rb").read(), dropbox_path)
        query = conn.execute(
            "insert into socialmedia(datasource,corpus) values('{0}','{1}')".format(datasource, corpus_length))
    else:
        query = conn.execute(
            "insert into socialmedia(datasource,corpus) values('{0}','{1}')".format(datasource, corpus_length))


def ssegwanga_scrapper():
    general_name = '_ssegwanga.csv'
    filename = time+general_name
    datasource = "Ssegwanga news"
    dropbox_path = "/Coast_data/"+filename
    link_urls = []
    corpus = []
    subhd = []
    hlines = []
    url = url = "https://sseggwanga.com/index.php/"+timez+"/"
    r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    soup = BeautifulSoup(r.content)
    snippet = soup.find_all('div', attrs={"class": "td-ss-main-content"})
    for i in snippet:
        for link in i.find_all('a'):
            line = link.get('href')
            link_urls.append(line)
    today_articles = list(set(link_urls))
    for x in today_articles:
        r = requests.get(x, headers={'User-Agent': 'Mozilla/5.0'})
        soup = BeautifulSoup(r.content)
        for division in soup.find_all("div", attrs={"class": "td-post-content"}):
            for link in division.find_all('p'):
                f = link.get_text()
                sentences = nltk.sent_tokenize(f)
                for sentence in sentences:
                    corpus.append(sentence)
            for header in soup.find_all("header", attrs={"class": "td-post-title"}):
                for link in header.find_all('h1'):
                    fh = link.get_text()
                    hsentences = nltk.sent_tokenize(fh)
                    for hsentence in hsentences:
                        hlines.append(hsentence)
                for subhead in header.find_all('p'):
                    fb = subhead.get_text()
                    bsentences = nltk.sent_tokenize(fb)
                    for bsentence in bsentences:
                        subhd.append(bsentence)
    hlines.extend(subhd)
    corpus.extend(hlines)
    df = pd.DataFrame(corpus, columns=['Luganda'])
    df.drop_duplicates(subset='Luganda', keep='first', inplace=True)
    df['Luganda'].replace('  ', np.nan, inplace=True)
    df = df.dropna(subset=['Luganda'])
    corpus_length = len(df)
    if corpus_length >= 2:
        df.to_csv(filename, index=False)
        client.files_upload(open(filename, "rb").read(), dropbox_path)
        query = conn.execute(
            "insert into socialmedia(datasource,corpus) values('{0}','{1}')".format(datasource, corpus_length))
    else:
        query = conn.execute(
            "insert into socialmedia(datasource,corpus) values('{0}','{1}')".format(datasource, corpus_length))


def dembe_scrapper():
    general_name = '_dembe.csv'
    filename = time+general_name
    dropbox_path = "/Coast_data/"+filename
    datasource = "Dembe FM"
    link_urls = []
    corpus_data = []
    sentx = []
    headz = []
    for page in range(1, 2):
        url = "https://www.dembefm.ug/category/amawulire/page/{}".format(page)
        r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        soup = BeautifulSoup(r.content)
        for division in soup.find_all("div", attrs={"class": "blog-archive"}):
            for link in division.find_all('h2', attrs={"class": "blog-arc-heading"}):
                for rel in link.find_all('a'):
                    pager = rel.get('href')
                    r = requests.get(
                        pager, headers={'User-Agent': 'Mozilla/5.0'})
                    soup = BeautifulSoup(r.content)
                    for division in soup.find_all("div", attrs={"class": "single-col"}):
                        for dayt in division.find_all("p", attrs={"class": "blog-date"}):
                            f = dayt.get_text()
                            data = f.replace("th", "")
                            dat = datetime.datetime.strptime(
                                data, '%B %d, %Y').strftime('%Y/%m/%d')
                            if dat == timez:
                                for link in division.find_all('h2'):
                                    f = link.get_text()
                                    sentences = nltk.sent_tokenize(f)
                                    for sentence in sentences:
                                        corpus_data.append(sentence)
                                for link in division.find_all('p'):
                                    fp = link.get_text()
                                    psentences = nltk.sent_tokenize(fp)
                                    for psentence in psentences:
                                        sentx.append(psentence)
    corpus_data.extend(sentx)
    df = pd.DataFrame(corpus_data, columns=['Luganda'])
    df.drop_duplicates(subset='Luganda', keep='first', inplace=True)
    df['Luganda'].replace('  ', np.nan, inplace=True)
    df = df.dropna(subset=['Luganda'])
    df = df[~df['Luganda'].str.split().str.len().lt(5)]
    corpus_length = len(df)
    if corpus_length >= 2:
        df.to_csv(filename, index=False)
        client.files_upload(open(filename, "rb").read(), dropbox_path)
        query = conn.execute(
            "insert into socialmedia(datasource,corpus) values('{0}','{1}')".format(datasource, corpus_length))
    else:
        query = conn.execute(
            "insert into socialmedia(datasource,corpus) values('{0}','{1}')".format(datasource, corpus_length))


def galaxyradio_scrapper():
    general_name = '_galaxyfm.csv'
    filename = time+general_name
    datasource = "Galaxy FM"
    dropbox_path = "/Coast_data/"+filename
    corpus_data = []
    headz = []
    for page in range(1, 2):
        url = "https://www.galaxyfm.co.ug/luganda/page/{}".format(page)
        r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        soup = BeautifulSoup(r.content)
        for division in soup.find_all("div", attrs={"class": "news-feed section group"}):
            for link in division.find_all('div', attrs={"class": "news-item col span_1_of_2"}):
                for rel in link.find_all('a'):
                    pager = rel.get('href')
                    if timez in pager:
                        r = requests.get(pager)
                        soup = BeautifulSoup(r.content)
                        for hdivision in soup.find_all("div", attrs={"class": "container body-block clear_fix mg-t-20 mg-b-10"}):
                            for hdlines in hdivision.find_all("h1", attrs={"class": "post-title"}):
                                hdline = hdlines.get_text()
                                headz.append(hdline)
                        for day_division in soup.find_all("div", attrs={"class": "post-content with-videos"}):
                            for mboz in day_division.find_all('p'):
                                mf = mboz.get_text()
                                msentences = nltk.sent_tokenize(mf)
                                for msentence in msentences:
                                    corpus_data.append(msentence)
    corpus_data.extend(headz)
    df = pd.DataFrame(corpus_data, columns=['Luganda'])
    df.drop_duplicates(subset='Luganda', keep='first', inplace=True)
    df['Luganda'].replace('  ', np.nan, inplace=True)
    df = df.dropna(subset=['Luganda'])
    df = df[~df['Luganda'].str.split().str.len().lt(5)]
    corpus_length = len(df)
    if corpus_length >= 2:
        df.to_csv(filename, index=False)
        client.files_upload(open(filename, "rb").read(), dropbox_path)
        query = conn.execute(
            "insert into socialmedia(datasource,corpus) values('{0}','{1}')".format(datasource, corpus_length))
    else:
        query = conn.execute(
            "insert into socialmedia(datasource,corpus) values('{0}','{1}')".format(datasource, corpus_length))


def simba_scrapper():
    general_name = '_simba.csv'
    filename = time+general_name
    datasource = "Radio Simba"
    dropbox_path = "/Coast_data/"+filename
    link_urls = []
    corpus_data = []
    sentx = []
    headz = []
    for page in range(1, 6):
        url = "https://www.radiosimba.ug/latest-news/page/{}".format(page)
        r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        soup = BeautifulSoup(r.content)
        for division in soup.find_all("div", attrs={"class": "mvp-main-body-blog left relative"}):
            for link in division.find_all('ul'):
                for paarg in link.find_all('li'):
                    for post_time in paarg.find_all('span', attrs={"class": "mvp-post-info-date left relative"}):
                        ptime = post_time.get_text()
                        duration = "hours"
                        if duration in ptime:
                            ptime = ptime.replace('/', '')
                            ptime = ptime.replace('hours', '')
                            ptime = ptime.replace('ago', '')
                            if int(ptime) <= 15:
                                for xlink in paarg.find_all('a'):
                                    pager = xlink.get('href')
                                    r = requests.get(
                                        pager, headers={'User-Agent': 'Mozilla/5.0'})
                                    soup = BeautifulSoup(r.content)
                                    for division in soup.find_all("div", attrs={"class": "theiaPostSlider_preloadedSlide"}):
                                        for link in division.find_all('p'):
                                            f = link.get_text()
                                            sentences = nltk.sent_tokenize(f)
                                            for sentence in sentences:
                                                corpus_data.append(sentence)
                                    for division in soup.find_all("div", attrs={"class": "_1mf _1mj"}):
                                        for link in division.find_all('span'):
                                            f = link.get_text()
                                            sentences = nltk.sent_tokenize(f)
                                            for sentence in sentences:
                                                sentx.append(sentence)
                                    for division in soup.find_all("div", attrs={"class": "left relative"}):
                                        for link in division.find_all('h1'):
                                            f = link.get_text()
                                            sentences = nltk.sent_tokenize(f)
                                            for sentence in sentences:
                                                headz.append(sentence)
    corpus_data.extend(sentx)
    headz.extend(corpus_data)
    major = list(set(headz))
    df = pd.DataFrame(major, columns=['Luganda'])
    df.drop_duplicates(subset='Luganda', keep='first', inplace=True)
    df['Luganda'].replace('  ', np.nan, inplace=True)
    df = df.dropna(subset=['Luganda'])
    # df = df[~df['Luganda'].str.split().str.len().lt(5)]
    corpus_length = len(df)
    if corpus_length >= 2:
        df.to_csv(filename, index=False)
        client.files_upload(open(filename, "rb").read(), dropbox_path)
        query = conn.execute(
            "insert into socialmedia(datasource,corpus) values('{0}','{1}')".format(datasource, corpus_length))
    else:
        query = conn.execute(
            "insert into socialmedia(datasource,corpus) values('{0}','{1}')".format(datasource, corpus_length))


# simba_scrapper()
# galaxyradio_scrapper()
# dembe_scrapper()
# ssegwanga_scrapper()
# gambuuze_scrapper()

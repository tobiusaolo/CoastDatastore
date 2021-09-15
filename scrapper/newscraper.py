from bs4 import BeautifulSoup, SoupStrainer, NavigableString, Tag
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
from nltk.tokenize.punkt import PunktSentenceTokenizer, PunktParameters
from pandas import DataFrame
timez = '2021/09/09'
time = '2021-09-09'


def gambuuze_scrapper():
    general_name = '_gambuuze.csv'
    filename = time+general_name
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
    df = df.dropna(subset=['Luganda'])
    # df = df[~df['Luganda'].str.split().str.len().lt(3)]
    df.to_csv(filename, index=False)


def ssegwanga_scrapper():
    general_name = '_ssegwanga.csv'
    filename = time+general_name
    link_urls = []
    corpus = []
    subhd = []
    hlines = []
    url = url = "https://sseggwanga.com/index.php/2021/09/13/"
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
    # df = df[~df['Luganda'].str.split().str.len().lt(3)]
    df.to_csv(filename, index=False)


ssegwanga_scrapper()
# gambuuze_scrapper()

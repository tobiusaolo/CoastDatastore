import schedule
from sqlalchemy import create_engine
import time
import datetime
import twint
import nest_asyncio
import pandas as pd
import numpy as np
import nltk
from nltk.tokenize import sent_tokenize
from nltk.tokenize.punkt import PunktSentenceTokenizer, PunktParameters
import re
from google.cloud import storage
import os
import glob
import psycopg2
import dropbox
dropbox_access_token = "-EBxOhuzOI4AAAAAAAAAAU1JAwmpo6Xy9PsYOguc1-mFh5QJsWAvENXusH06oWXr"
client = dropbox.Dropbox(dropbox_access_token)


# db_connect = create_engine(
# 'postgresql://awjzgmwqiatzjg:e4424ae3d375e2057bcc9cde832672940d44ea2c05260e28ccb04dc1575ec52d@ec2-34-204-22-76.compute-1.amazonaws.com:5432/dabbhqt4pegslv')
# conn = db_connect.connect()
punkt_param = PunktParameters()
abbreviation = ['hon', 'rt', 'col', 'lt', 'maj', 'gen', 'fr']
punkt_param.abbrev_types = set(abbreviation)
tokenizer = PunktSentenceTokenizer(punkt_param)
timez = datetime.datetime.today().strftime("%Y-%m-%d")
general_name = '_socialmedia.csv'
filename = timez+general_name
dropbox_path = "/Coast_data/"+filename
# print(timez)


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))


def socialmedia():
    copora = []
    datapoint = ['BANKOSAMUBS', 'BugandaOfficial', 'cpmayiga', 'bbstvug', 'gambuuze', 'UKBuganda',
                 '892cbsFm', 'bukeddetv', 'bukeddeonline', 'DembeFm', 'simbaradio', 'sparktvuganda', 'radiobuddu98', 'BeatFMUganda', 'CbsfmUg']
    # datapoint = ['BugandaOfficial', 'cpmayiga']
    # Configure
    nest_asyncio.apply()
    for sourcehandle in datapoint:
        c = twint.Config()
        # c.Search = "from:bbstvug"
        c.Username = sourcehandle
        # c.Limit = 10
        c.Lang = 'en'
        c.Since = '2021-09-14'
        c.Until = '2021-09-16'
        c.Store_json = True
        c.Hide_output = True
        c.Output = "MOH.json"
        c.Pandas = True
        twint.run.Search(c)
        df = twint.storage.panda.Tweets_df
        try:
            df = df['tweet']
            remove_special_characters = re.compile('[^0-9a-z +]')
            moh = []
            scrapedlist = []
            mohlist = df.to_list()
            for x in mohlist:
                x = re.sub('@', '', x)
                x = re.sub(r"http\S+", "", x)
                #     x=':'.join(x.split(':')[1:])
                x = re.sub(r"[a-zA-Z]\.\s+", "", x)
                #     x = re.sub(remove_special_characters, '', x)
                x = tokenizer.tokenize(x)
                moh.append(x)
            for y in moh:
                for i in y:
                    scrapedlist.append(i)
            betadf = pd.DataFrame({'Sentences': scrapedlist})
            betadf['Sentences'] = betadf['Sentences'].map(
                lambda x: x.lstrip('@'))
            betadf = betadf[~betadf['Sentences'].str.split().str.len().lt(6)]
            print(len(betadf))
            gh = betadf['Sentences'].to_list()
            print("******* handle*******  "+sourcehandle)
            for xy in gh:
                copora.append(xy)
                # print(xy)
                # query = conn.execute(
                # "insert into socialmedia(datasource,sentence) values('{0}','{1}')".format(sourcehandle, xy))
        except:
            print(sourcehandle + " is empty")
    # print(len(copora))
    df = pd.DataFrame(copora, columns=['Luganda'])
    df.drop_duplicates(subset='Luganda', keep='first', inplace=True)
    df['Luganda'].replace('  ', np.nan, inplace=True)
    df = df.dropna(subset=['Luganda'])
    # df = df[~df['Luganda'].str.split().str.len().lt(3)]
    df.to_csv(filename, index=False)
    client.files_upload(open(filename, "rb").read(), dropbox_path)


socialmedia()
# filename = "2021-09-13_socialmedia.csv"
# upload_blob("gambuuzeug", filename, filename)
# schedule.every(20).seconds.do(socialmedia)
# while 1:
# schedule.run_pending()
# time.sleep(1)s

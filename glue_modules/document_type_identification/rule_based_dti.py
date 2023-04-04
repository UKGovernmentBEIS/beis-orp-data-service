#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 16 16:54:23 2023

@author: imane.hafnaoui
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
from pyspark import SparkFiles
dtypes = ['HS-1', 'GD0', 'HS0', 'MSI0', 'GD1', 'HS1']
dti_map = dict(zip(dtypes, ['HS', 'GD', 'HS', 'MSI', 'GD', 'HS']))
inv_dtd = dict(enumerate(dtypes))
dtd = {v: k for k, v in inv_dtd.items()}

DTI_CHUNK_PERC = 0.1
# DTI_GOV_MAP_PATH = '/Users/imane.hafnaoui/gitpod/MXTrepos/beis-orp-data-service/glue_modules/document_type_identification/govuk_document_type.csv'
DTI_GOV_MAP_PATH = SparkFiles.get('resources/govuk_document_type.csv')
df = pd.read_csv(DTI_GOV_MAP_PATH).fillna('NA')


def extract_DT(nlp, q):
    doc = nlp(q.lower())
    return [ent.label_ for ent in doc.ents]


def dti_text(text, title, nlp):

    full_text = text if title.lower() in text.lower() else title+'. ' + text

    possible_types = extract_DT(
        nlp, full_text[:max(2000, int(len(full_text) * DTI_CHUNK_PERC))])
    if possible_types:
        return dti_map[inv_dtd[max(map(dtd.get, possible_types))]]
    else:
        return 'NA'


def dti_web(url, parent_url):
    try:
        gov = 'https://www.gov.uk/'
        hse = 'https://www.hse.gov.uk/'
        if url.startswith(hse):
            possible_dt = BeautifulSoup(requests.get(url).content,features="lxml").findAll('body')[
                0].attrs.get('class')
        elif url.startswith(gov):
            furl = f'{gov}api/content/{url.split(gov)[-1]}'
            js = requests.get(furl).json()
            pt = js['links'].get('parent')
            pdt = [i['document_type'] for i in pt] if pt else []
            possible_dt = [js['document_type']]+pdt
        elif parent_url:
            possible_dt = dti_web(parent_url, None)
        else: 
            possible_dt = []

        dt = df[df.document_type.isin(possible_dt)].orp_dt
        return dt.iloc[0] if len(dt) > 0 else 'NA'
    except Exception as e:
        print(f'ERROR [DTI]: {url}\n {e}')
        return 'NA'

def dti(url,parent_url,  text, title, nlp):
    dt = dti_web(url, parent_url)
    if dt == 'NA':
        dt = dti_text(text, title, nlp)
    return dt

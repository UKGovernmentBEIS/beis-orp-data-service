#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 16 16:54:23 2023

@author: imane.hafnaoui
"""

import spacy
import pandas as pd
from datetime import datetime

dtypes = ['HS-1','GD0','HS0','MSI0', 'GD1','HS1']
rg = range(len(dtypes))
inv_dtd = dict(zip(rg, dtypes))
dtd = dict(zip(dtypes, rg))

CHUNKPERC=0.1

def DTI(df, rulebook, testing=False):

    def extract_DT(q):
        doc = nlp(q.lower())
        return [(ent.label_, ent.start, ent.text) for ent in doc.ents]

    nlp = spacy.load("en_core_web_sm", exclude=['entity_ruler',  'ner'])
    nlp.add_pipe("entity_ruler", config={'phrase_matcher_attr':'LOWER'}).from_disk(rulebook)


    colnames = ['ndt_title','ndt_text'] if testing else ['dt_title','dt_text']
    print('processing titles...')
    df['dt_title_org'] = df.title.apply(extract_DT)
    # df['dt_title']=df.dt_title_org.apply(lambda x: x[0][0] if x else None)
    print('processing text...')
    df['text'] = df.apply(lambda x: x.title+'. '+ x.text if x.title.lower() not in x.text.lower() else x.text, axis =1)
    df['dt_text_org'] = df.text.apply(lambda x: extract_DT(x[:max(2000, int(len(x)*CHUNKPERC))]))

    df[colnames]=df[['dt_title_org','dt_text_org']].applymap(lambda x: inv_dtd[max([dtd[i[0]] for i in x])] if x else None)

    return df

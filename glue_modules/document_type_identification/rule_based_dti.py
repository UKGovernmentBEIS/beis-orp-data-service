#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 16 16:54:23 2023

@author: imane.hafnaoui
"""


dtypes = ['HS-1','GD0','HS0','MSI0', 'GD1','HS1']
dti_map = dict(zip(dtypes, ['HS','GD','HS','MSI','GD','HS']))
inv_dtd = dict(enumerate(dtypes))
dtd = {v:k for k,v in inv_dtd.items()}

DTI_CHUNK_PERC=0.1

def extract_DT(nlp, q):
    doc = nlp(q.lower())
    return [ent.label_ for ent in doc.ents]


def dti(text, title, nlp):
     
    full_text = text if title.lower() in text.lower() else title+'. '+ text

    possible_types = extract_DT(nlp, full_text[:max(2000, int(len(full_text) * DTI_CHUNK_PERC))])
    return dti_map[inv_dtd[max(map(dtd.get, possible_types))]]
 

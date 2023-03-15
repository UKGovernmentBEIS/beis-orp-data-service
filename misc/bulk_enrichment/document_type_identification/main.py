#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 16 16:54:23 2023

@author: imane.hafnaoui
"""

import pandas as pd
from datetime import datetime
from rule_based_DTI import DTI

override=False
rulebook1 = "data/doc_type_rules_v.1.jsonl"
rulebook2 = "data/doc_type_rules_v.2.jsonl"
datapath='data/matching_results_2023-03-13T14:55:30.504104.p'
# datapath = 'data/bulk-data-2023-03-08-full-nodup.p'
svpath = f"data/matching_results_{datetime.now().isoformat()}.p"


df = pd.read_pickle(datapath)
if override:
    df = DTI(df, rulebook1)
    
df = DTI(df, rulebook2, True)


cov = df.dt_text.dropna().shape[0]
ncov = df.ndt_text.dropna().shape[0]
n = df.shape[0]

df.to_pickle(svpath)

t=f'''
Previous:    
    Tagged documents: {cov}/{n} \n
    Coverage:         {cov/n*100} \n
    
Update:\n
    Tagged documents: {ncov}/{n} \n
    Coverage:         {ncov/n*100} \n
    Distribution: \n
        {df.ndt_text.fillna('NA').value_counts()/n*100}
      '''
print(t)
open(svpath+'.txt','w').write(t)
      
'''
- check MSI -> GD
     possible msi: 
         ed3612eb91344f87b88d808b71d4678f
         d4fdadd46fab43918553f67f0ce60fda
     '''
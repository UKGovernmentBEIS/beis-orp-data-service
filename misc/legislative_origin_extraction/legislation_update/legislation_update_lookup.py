# -*- coding: utf-8 -*-
"""
Created on Mon Feb 28 13:57:04 2022

@author: Imane.Hafnaoui

Script to collect new legislation act published since `date`. The Sparql query is limited to UKPGA legislations and
retrieves;
    - document title (as href)
    - versions (as href)
    - short title (could vary for different versions - not always present especially for old legislations)
    - citation/acronymcitation (different ways the act has been cited)
    - year of legislation publication

Parameters:
    keypath : path to credentials to access legistlation sparql endpoint. [format - username:password]
    savefile : file path to store returned data. [format - path\to\file.csv]
    date : date to indicate the last time the update checkup was performed [format - YYYY-mm-ddTHH:MM:SS]

Returns:
    df : Dataframe with new acts since `date`.
"""
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, CSV
from io import BytesIO
import sys


LEG_DIV = 'leg-division-list.csv'
dff = pd.read_csv(LEG_DIV)

if __name__ == "__main__":
    args = sys.argv[1:]
    keypath = args[0]
    savefile = args[1]
    date = args[2]

    sparql = SPARQLWrapper("https://www.legislation.gov.uk/sparql")
    usname, pw = open(keypath).read().strip('\n').split(':')
    sparql.setCredentials(user=usname, passwd=pw)
    sparql.setReturnFormat(CSV)
    df = pd.DataFrame()
    sparql.setQuery("""
                prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                prefix xsd: <http://www.w3.org/2001/XMLSchema#>
                prefix void: <http://rdfs.org/ns/void#>
                prefix dct: <http://purl.org/dc/terms/>
                prefix sd: <http://www.w3.org/ns/sparql-service-description#>
                prefix prov: <http://www.w3.org/ns/prov#>
                prefix leg: <http://www.legislation.gov.uk/def/legislation/>
                select distinct ?ref  ?title ?href ?shorttitle ?citation ?acronymcitation ?year ?number
                where {
                   ?activity prov:endedAtTime ?actTime .
                   ?graph prov:wasInfluencedBy ?activity .
                   ?activity rdf:type <http://www.legislation.gov.uk/def/provenance/Addition> .
                   ?dataUnitDataSet sd:namedGraph ?graph .
                   <http://www.legislation.gov.uk/id/dataset/topic/core> void:subset ?dataUnitDataSet .
                   graph ?graph { ?ref a leg:Legislation;
                                        leg:title ?title ;
                                        leg:year ?year ;
                                        leg:interpretation ?href .
                                   OPTIONAL {?ref   leg:citation ?citation  } .
                                   OPTIONAL {?ref   leg:acronymCitation ?acronymcitation} .
                                   OPTIONAL {?href  leg:shortTitle ?shorttitle} .
                                   OPTIONAL {?ref   leg:number ?number  } .}
                   FILTER(str(?actTime) > "%s")
                }
                """ % date)

    results = sparql.query().convert()
    df = pd.read_csv(BytesIO(results))
    stitles = ['title', 'shorttitle', 'citation', 'acronymcitation']
    df['candidate_titles'] = df[stitles].apply(list, axis=1)
# ====
    df['divAbbv'] = df.ref.apply(lambda x: x.split('/')[4])
    df = df.merge(dff[['legDivision', 'legType', 'divAbbv']], how='left')
# ====
    df = df.explode('candidate_titles')
    df = df[~df['candidate_titles'].isna()].drop_duplicates('candidate_titles')
    df.to_csv(savefile, index=None)

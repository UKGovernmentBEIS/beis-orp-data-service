#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 15 10:15:34 2022

@author: imane.hafnaoui
"""

import json
import logging
import os, re
from typedb.client import TransactionType, SessionType, TypeDB
from datetime import datetime
from itertools import groupby
import pandas as pd
from word_forms_loc.word_forms_loc import get_word_forms
from word_forms_loc.lemmatizer import lemmatize


search_keys = {"id", "keyword", "title", "date_published",
               "regulator_id", "status", "regulatory_topic", "document_type",
               "legislation_href"}
return_vals = ['title', 'summary', 'document_uid', 'regulator_id',"regulatory_topic",
               'document_type', 'keyword', 'uri', 'status', 'language',
               'date_published', 'date_uploaded', 'legislative_origins', 'version']
leg_vals = ['href', 'title', 'leg_type', 'leg_division']
sp_chars = r'"|,|;'
RET_SIZE = 10

LOGGER = logging.getLogger()
LOGGER.setLevel(int(os.environ.get("LOGGING_LEVEL", logging.INFO)))


###########################################
# HELPER FUNCTIONS
###########################################

def validate_env_variable(env_var_name):
    LOGGER.debug(
        f"Getting the value of the environment variable: {env_var_name}")
    try:
        env_variable = os.environ[env_var_name]
    except KeyError:
        raise Exception(f"Please, set environment variable {env_var_name}")
    if not env_variable:
        raise Exception(f"Please, provide environment variable {env_var_name}")
    return env_variable

def clean_text(text): 
    if type(text)==list:
        return [clean_text(i) for i in text]
    elif type(text)==str:
        return re.sub(sp_chars, '', text).encode("ascii", "ignore").decode()
    else: return text

def format_datetime(date): return datetime.isoformat(date)


def get_select_dict(results: dict, selc: list): return {k: (format_datetime(
    v) if type(v) == datetime else v)for k, v in results.items() if (v!='') & (k in selc)}


def remap(d: dict, mapd: dict): return {
    mapd.get(k, k): v for k, v in d.items()}


def group_attributes(attr):
    results = []
    for k, gp in groupby(attr, lambda x: x[0]):
        gpl = list(gp)
        results.append((k, [i[1] for i in gpl] if len(gpl) > 1 else gpl[0][1]))
    return dict(results)


def getUniqueResult(results):
    res = [(i.get_type().get_label().name(), i.get_value())
           for a in results for i in a.concepts() if i.is_attribute()]
    return group_attributes(res)

def group_of_group(results, id='id', grouping='y', attribute='attribute'):
    ret = {}
    for res in results:
        a = []
        gp1 = res.concept_maps()[0].map()[id].get_value()
        attrs = [i.map() for i in res.concept_maps()]
        df=pd.DataFrame([(i[grouping].get_iid(), (i[attribute].get_type().get_label().name(), i[attribute].get_value())) for i in attrs ])
        ret[gp1] = df.groupby(0)[1].apply(list).apply(group_attributes).to_list()
    return ret


def matchquery(query, session, group=True):
    with session.transaction(TransactionType.READ) as transaction:
        print("Query:\n %s" % query)
        iterator = transaction.query().match_group(
            query) if group else transaction.query().match(query)
        results = [ans for ans in iterator]
        return results


def get_lemma(word):
    # return word
    try:
        return lemmatize(word)
    except ValueError as err:
        if 'is not a real word' in err.args[0]:
            return word
        else:
            raise ValueError(err)


def lemma2noun(lemma):
    # return lemma
    nn = list(get_word_forms(lemma).get('n', []))
    return sorted(nn, key=len)[0] if nn else lemma

############################################
# LAMBDA HANDLER
############################################


def query_builder(event):
    event = {k:clean_text(v) for k,v in event.items() }
    # Build TQL query from search params
    subq = ""
    query = 'match $x isa regulatoryDocument, has attribute $attribute'
    # Document API
    if event.get('id'):
        query += f', has document_uid "{event["id"].lower()}"'

    # Related reg docs (links to legislation search)
    elif event.get('legislation_href'):
        query = 'match $x isa legislation, has URI $id; $id like "'
        query += '|'.join([leg for leg in event.get('legislation_href', [])])
        query += '''";  $regdoc isa regulatoryDocument, has attribute $attribute;
            (issuedFor:$x,issued:$regdoc) isa publication; limit 1000;
            group $x;'''
        return query

    # Search API
    else:
        # simple filters
        if event.get('regulatory_topic'):
            query += f', has regulatory_topic "{event["regulatory_topic"]}"'

        # list filters
        if event.get('keyword'):
            query += ''.join(
                [f', has keyword "{get_lemma(kw.strip().lower())}"' for kw in event['keyword'].split(' ')])

        if event.get('document_type'):
            query += ', has document_type $document_type'
            subq += f"; $document_type like \"{'|'.join([i for i in event['document_type']])}\""

        if event.get('status'):
            query += ', has status $status'
            subq += f"; $status like \"{'|'.join([i for i in event['status']])}\""

        # compound filters
        if event.get('date_published'):
            date = event["date_published"]
            st = date.get('start_date')
            ed = date.get('end_date')
            if st:
                query += f', has date_published >= {st}'
            if ed:
                query += f', has date_published <= {ed}'

        if event.get('title'):
            query += ', has title $title'
            subq += f'; $title contains "{event["title"].lower()}"'

        if event.get('regulator_id'):
            query += '; $y isa regulator, has regulator_id $rid'
            subq += f"; $rid like \"{'|'.join([i for i in event['regulator_id']])}\""
            query += '; (issued:$x,issuedBy:$y) isa publication'
    query += subq
    query += '; get $attribute, $x; group $x;'
    return query


def search_reg_docs(ans, page_size):
    # -> [{leg_href:string, related_docs:[]}]
    res = group_of_group(ans, grouping='regdoc')
    docs = []
    for leg, regdocs in res.items():
        doc = {'legislation_href': leg}
        data = []
        for rd in regdocs[:page_size]:
            rd['keyword'] = list(set([lemma2noun(kw)
                                 for kw in rd.get('keyword', [])]))
            aot = rd.get('assigned_orp_topic')
            if aot:
                rd['regulatory_topic'] = max(aot, key=lambda x: len(x.split('/'))) if type(aot)==list else aot
            data.append(get_select_dict(rd, return_vals))
        doc['related_docs'] = data
        docs.append(doc)
    return docs


def search_leg_orgs(ans, session):
    res = pd.DataFrame([dict(getUniqueResult(a.concept_maps()))
           for a in ans])

    # Query the graph database for legislative origins
    LOGGER.info("Querying the graph for legislative origins")

    query = 'match $x isa regulatoryDocument, has node_id $id; $id like "'
    query += '|'.join(res['node_id'])
    query += '''";  $leg isa legislation, has attribute $attribute;
        (issuedFor:$leg,issued:$x) isa publication;
        group $x;'''
    ans = matchquery(query=query, session=session)
    legs = pd.DataFrame(group_of_group(ans, grouping='leg').items(), columns=['node_id', 'legislative_origins'])

    # Merging leg.orgs info with reg document
    df = res.merge(legs,on='node_id', how='left')
    legmap = {'leg_type': 'type', 'leg_division': 'division'}
    df.legislative_origins = df.legislative_origins.fillna("").apply(list).apply(lambda x: list(
            filter(None, [remap(get_select_dict(a, leg_vals), legmap) for a in x])))

    # get noun for keywords
    df.keyword = df.keyword.apply(lambda x: list(set([lemma2noun(kw) for kw in x]) if x else []))

    # get assigned topic
    if 'assigned_orp_topic' in df.columns: 
        df.regulatory_topic = df.assigned_orp_topic.apply(lambda aot: max(aot, key=lambda x: len(x.split('/'))) if type(aot)==list else aot)

    df = df.applymap(lambda x: format_datetime(x) if isinstance(x, pd.Timestamp) else x)
    return df.fillna('').apply(lambda x: get_select_dict(x, return_vals), axis=1).tolist()


def search_module(event, session):
    keyset = set(event.keys()) & search_keys
    page_size = int(event.get('page_size', RET_SIZE))
    page = int(event.get('page', 0)) * page_size

    if len(keyset) == 0:
        return {
            "status_code": 400,
            "status_description": "Bad Request - Unsupported search parameter(s)."
        }

    else:
        # Build TQL query from search params
        query = query_builder(event)

        # Query the graph database for reg. documents
        ans = matchquery(query, session)
        num_ret = len(ans)

        LOGGER.info(f"Ret -> {num_ret}")

        # second hop search
        if event.get('legislation_href'):
            LOGGER.info("Querying the graph for related reg. documents")
            docs = search_reg_docs(ans, page_size)
        else:
            LOGGER.info("Querying the graph for reg. documents")
            docs = search_leg_orgs(ans[page:page + page_size], session)

        return {
            "status_code": 200,
            "status_description": "OK",
            "total_search_results": num_ret,
            "documents": docs
        }


def lambda_handler(ev, context):
    LOGGER.info("Received event: " + json.dumps(ev, indent=2))

    event = json.loads(ev['body'])

    LOGGER.info("Event Body: ", event)
    TYPEDB_IP = validate_env_variable('TYPEDB_SERVER_IP')
    TYPEDB_PORT = validate_env_variable('TYPEDB_SERVER_PORT')
    TYPEDB_DATABASE_NAME = validate_env_variable('TYPEDB_DATABASE_NAME')

    client = TypeDB.core_client(TYPEDB_IP + ':' + TYPEDB_PORT)
    session = client.session(TYPEDB_DATABASE_NAME, SessionType.DATA)
    result = search_module(event, session)
    return result

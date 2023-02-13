#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 15 10:15:34 2022

@author: imane.hafnaoui
"""

import json
import logging
import os
from typedb.client import TransactionType, SessionType, TypeDB
from datetime import datetime
from itertools import groupby
# from word_forms_loc.word_forms_loc import get_word_forms
# from word_forms_loc.lemmatizer import lemmatize

search_keys = {"id", "keyword", "title", "data_published",
               "regulator_id", "status", "regulatory_topic", "document_type"}
return_vals = ['title', 'summary', 'document_uid', 'regulator_id',
               'document_type', 'keyword',  'object_key',
               'date_published', 'date_uploaded', 'legislative_origins', 'version']
leg_vals = ['url', 'title', 'leg_type', 'leg_division']
RET_SIZE = 10

LOGGER = logging.getLogger()
LOGGER.setLevel(int(os.environ.get("LOGGING_LEVEL", logging.INFO)))


############################################
# HELPER FUNCTIONS
############################################

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

def format_datetime(date): return datetime.strftime(date, "%Y-%m-%dT%H:%M:%S") #TODO remove this tabun

def get_select_dict(results: dict, selc: list): return {k: (format_datetime(
    v) if type(v) == datetime else v)for k, v in results.items() if k in selc}

def getUniqueResult(results):
    res = [(i.get_type().get_label().name(), i.get_value())
           for a in results for i in a.concepts() if i.is_attribute()]
    results = []
    for k, gp in groupby(res, lambda x: x[0]):
        gpl = list(gp)
        results.append((k, [i[1] for i in gpl] if len(gpl) > 1 else gpl[0][1]))
    return results


def matchquery(query, session, group=True):
    with session.transaction(TransactionType.READ) as transaction:
        print("Query:\n %s" % query)
        iterator = transaction.query().match_group(
            query) if group else transaction.query().match(query)
        results = [ans for ans in iterator]
        return results


def get_lemma(word):
    return word
    # try:
    #     return lemmatize(word)
    # except ValueError as err:
    #     if 'is not a real word' in err.args[0]:
    #         return word
    #     else:
    #         raise ValueError(err)

def lemma2noun(lemma):
    return lemma
    # nn = list(get_word_forms(lemma).get('n', []))
    # return sorted(nn, key=len)[0] if nn else lemma
   
############################################
# LAMBDA HANDLER
############################################

def query_builder(event):

    # Build TQL query from search params
    subq = ""
    query = 'match $x isa regulatoryDocument, has attribute $attribute'
    # Document API
    if event.get('id'):
        query += f', has document_uid "{event["id"].lower()}"'

    # Search API
    else:
        # simple filters
        if event.get('regulatory_topic'):
            query += f', has regulatory_topic "{event["regulatory_topic"]}"'
        if event.get('status'):
            query += f', has status "{event["status"]}"'

        # list filters
        if event.get('keyword'):
            query += ''.join(
                [f', has keyword "{get_lemma(kw.strip().lower())}"' for kw in event['keyword'].split(' ')])

        # compound filters
        if event.get('date_published'):
            st, ed = event["date_published"]
            query += f', has date_published >= {st}'
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


def search_module(event, session):
    keyset = set(event.keys()) & search_keys
    page = int(event.get('page', 0))*RET_SIZE
    if len(keyset) == 0:
        return {
            "status_code": 400,
            "status_description": "Bad Request - Unsupported search parameter(s)."
        }

    else:
        # Build TQL query from search params
        query = query_builder(event)

        # Query the graph database for reg. documents
        LOGGER.info("Querying the graph for reg. documents")
        ans = matchquery(query, session)
        num_ret = len(ans)
        res = [dict(getUniqueResult(a.concept_maps()))
               for a in ans[page:page+RET_SIZE]]
        LOGGER.info(f"Ret -> {num_ret}")

        # Query the graph database for legislative origins
        LOGGER.info("Querying the graph for legislative origins")
        for doc in res:
            query = f'match $x isa regulatoryDocument, has node_id "{doc["node_id"]}";' + \
                '$y isa entity, has attribute $attribute;' + \
                ' ($x,$y) isa publication;' + \
                ' get $attribute, $y; group $y;'
            ans = [dict(getUniqueResult(a.concept_maps()))
                   for a in matchquery(query, session)]

            doc['keyword'] = set([lemma2noun(kw) for kw in doc.get('keyword', [])])
            doc['legislative_origins'] = list(
                filter(None, [get_select_dict(a, leg_vals) for a in ans]))
            doc['regulator_id'] = list(
                filter(None, [a.get('regulator_id') for a in ans]))[0]

        docs = [get_select_dict(doc, return_vals) for doc in res]
        out = {
            "status_code": 200,
            "status_description": "OK",
            "total_search_results": num_ret,
            "documents": docs
        }
        return out

def lambda_handler(ev, context):
    LOGGER.info("Received event: " + json.dumps(ev, indent=2))

    event = json.loads(ev['body'])

    LOGGER.info("Event Body: ", event)
    TYPEDB_IP = validate_env_variable('TYPEDB_SERVER_IP')
    TYPEDB_PORT = validate_env_variable('TYPEDB_SERVER_PORT')
    TYPEDB_DATABASE_NAME = validate_env_variable('TYPEDB_DATABASE_NAME')

    client = TypeDB.core_client(TYPEDB_IP + ':'+TYPEDB_PORT)
    session = client.session(TYPEDB_DATABASE_NAME, SessionType.DATA)
    result = search_module(event, session)
    return result

# -*- coding: utf-8 -*-
"""
Created on Wed Aug 24 15:33:34 2022

@author: imane.hafnaoui
"""
from utils.tdb_query_helpers import  *
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

# =====

# todo  convert time to utc
def changedAttrs(old:dict, new:dict, attr_type_dict):
    def format(v, atype, ref):
        if (isinstance(v, list)):
            # all([match(k, i, atype)!=ref for i in v]) TODO modify to handle lists
            return False
        elif (atype=='datetime'): return to_datetime(v).isoformat().replace('+00','') != str(ref)
        else: return str(v).strip() != str(ref).strip()
    return [(k, v) for k,v in new.items() if (format(v,attr_type_dict[k], old.get(k, None)))&(v!=None)]

def sim_hash(in_attr, db_attr):
    in_hash = np.array(map(int, in_attr['hash_text'].split('_')))
    db_hash = np.array(map(int, db_attr['hash_text'].split('_')))
    return cosine_similarity(in_hash, db_hash)

def version_handler():

    return

# ======
def updateE(etype, identifier, attrs, db_attrs, attr_type_dict, dict_thing_attrs):
    query = ""
    dquery = ""
    db_attrs_dict = dict(db_attrs)
    # isolate attrs that have changed/are new
    changed_attrs = changedAttrs(db_attrs_dict, dict(attrs), attr_type_dict)
    logger.debug(f"==> Changed attrs for {etype} -> \n {changed_attrs}")
    if changed_attrs:
        # update entity
        query = match_insert_ent(etype, identifier, changed_attrs, attr_type_dict)
    return [query, dquery]

def insertE(etype, attrs, attr_type_dict):
    logger.info(f"==> inserting new entity {etype}")
    q = f"$_ isa {etype}"
    q += formatAttrDB(attrs, attr_type_dict)
    q += ";"
    return q

def insertR(rtype, nodes, attrs, attr_type_dict):
    logger.info(f"==> inserting relation {rtype}")
    query = "match" 
    query +=  "".join([f" $x{i} isa {etype} \
     {''.join([match(k,v, attr_type_dict[k]) for k,v in eID])} ;" for i, (etype, eID, _) in enumerate(nodes)]) 
    query += f"insert ({','.join([f'{role}:$x{i}' for i, (_,_,role) in enumerate(nodes)])}) isa {rtype}"
    query += formatAttrDB(attrs, attr_type_dict)
    query += ";"
    return query

# ====
def processEntities(nodes, attr_type_dict, dict_thing_attrs, session):
    queries = []
    mqueries = []
    logger.info(f"Processing Entities ")
    logger.debug(f"-> {nodes}")
    for etype, identifier, attrs in nodes:
        logger.info(f"? ==> Checking entity {etype} exists" )
        db_ent = getEntityDB(etype, identifier, attr_type_dict, session)
        logger.debug(f"DB STATS [{etype, identifier}]: %s"%('\n'.join(db_ent)))
        if db_ent:
            logger.info(f"{etype} exists! -> updating...")
            db_attr_dict = dict(db_ent)
            in_attr_dict = dict(attrs)
            if sim_hash(in_attr_dict, db_attr_dict) >= 0.99:
                # entity exists with slight diff -> merge
                mqueries.extend(updateE(etype, identifier, in_attr_dict, db_attr_dict, attr_type_dict, dict_thing_attrs))
            else:
                
        else:
            # insert a new entity
            queries.append(insertE(etype, attrs, attr_type_dict))
    return queries, mqueries

def processLinks(links, attr_type_dict, session):
    mqueries = []
    logger.info(f"Processing Links ")
    logger.debug(f"->  {links}")
    for rtype,  check, nodes, attrs in links:
        if check:
            # check link exists
            logger.info(f"? ==> Checking relation {rtype} exists" )
            db_rel = getRelationDB(rtype, [], nodes, attr_type_dict, session, check=True)
            logger.debug(f"Return: {db_rel}")
            if not db_rel:
                # insert new link
                mqueries.append(insertR(rtype, nodes, attrs, attr_type_dict))
        else:
            # insert new link
            mqueries.append(insertR(rtype, nodes, attrs, attr_type_dict))
    return mqueries




def process_record(jsobj, attr_type_dict, dict_thing_attrs, session): 
    queries = []
    mqueries = []
    
    q, mq = processEntities(jsobj.get('entities', []), attr_type_dict, dict_thing_attrs, session)
    queries.extend(q)
    mqueries.extend(mq)
    mqueries.extend(processLinks(jsobj.get('links', []), attr_type_dict, session))
    return queries, mqueries

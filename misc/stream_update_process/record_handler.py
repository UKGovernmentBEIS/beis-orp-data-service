# -*- coding: utf-8 -*-
"""
Created on Wed Aug 24 15:33:34 2022

@author: imane.hafnaoui
"""
from utils.tdb_query_helpers import  *
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity


logging.getLogger().addHandler(logging.StreamHandler())
# =====

def changedAttrs(old:dict, new:dict, attr_type_dict):
    def format(v, atype, ref):
        print(v, ref)
        if (isinstance(v, list)):
            sref = set(ref) if ref else set()
            return len(set(v)^sref) != 0
        elif (atype=='datetime'): 
            return to_datetime(v) != ref
        else: return clean_text(str(v)) != str(ref)
    return  {k: v for k,v in new.items() if (format(v, attr_type_dict[k], old.get(k, None)))&(v!=None)}

def sim_hash(in_attr, db_attr):
    in_hash = np.array(in_attr['hash_text'].split('_')).reshape(1, -1)
    db_hash = np.array(db_attr['hash_text'].split('_')).reshape(1, -1)
    return cosine_similarity(in_hash, db_hash)

# ======
def updateE(etype, identifier, attrs, db_attrs, attr_type_dict):
    query = ""
    mquery = ""
    dquery = ""

    db_attr_dict = dict(db_attrs)
    in_attr_dict = dict(attrs)
    # isolate attrs that have changed/are new
    changed_attrs = changedAttrs(db_attr_dict, in_attr_dict, attr_type_dict)
    logger.debug(f"==> Changed attrs for {etype} -> \n {changed_attrs}")
    print(f"==> Changed attrs for {etype} -> \n {changed_attrs}")

    if changed_attrs:
        similarity = sim_hash(in_attr_dict, db_attr_dict)
        print(f"HASH SIMILARITY: {similarity}")
        if (etype == 'regulatoryDocument') and (similarity < 0.99):
            logger.debug('-- Entity exists with big changes -> [NEW VERSION]')
            print('-- Entity exists with big changes -> [NEW VERSION]')
            # compile new attrs
            new_attrs = db_attr_dict.copy()
            new_attrs.update(in_attr_dict)
            # version updating
            new_attrs['version'] = int(new_attrs.get('version', 1)) + 1 
            query = insertE(etype, new_attrs.items(), attr_type_dict)
            # change old vers' status -> archive
            dquery = deleteAttrOwn(etype=etype, 
                                identifier=db_attr_dict['document_uid'],
                                attrs=[('status', db_attr_dict['status'])], 
                                in_attrs=[('status', 'archive')], 
                                attr_type_dict=attr_type_dict)
            
        else:
            logger.debug('-- Entity exists with slight diff -> [MERGE]')
            print('-- Entity exists with slight diff -> [MERGE]')
            # changed_attrs.pop("hash_text", None)
            # delete old attributes
            dquery = deleteAttrOwn(etype=etype, 
                                identifier=db_attr_dict['document_uid'],
                                attrs=[(k,v) for k,v in db_attr_dict.items() if k in changed_attrs.keys()],  
                                attr_type_dict=attr_type_dict)
            # update entity
            mquery = match_insert_ent(etype, identifier, changed_attrs.items() , attr_type_dict)
    return [query,mquery, dquery]

def insertE(etype, attrs, attr_type_dict):
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
def processEntities(nodes, attr_type_dict, session):
    queries = []
    mqueries = []
    dqueries = []
    logger.info(f"Processing Entities ")
    logger.debug(f"-> {nodes}")
    for etype, identifier, attrs in nodes:
        logger.info(f"? ==> Checking entity {etype} exists" )
        db_ent = getEntityDB(etype, identifier, attr_type_dict, session)
        logger.debug(f"DB STATS [{etype, identifier}]: %s"%('\n'.join(db_ent)))
        if db_ent:
            logger.info(f"{etype} exists! -> updating...")
            q,mq, dq = updateE(etype, identifier, attrs, db_ent, attr_type_dict)
            queries.append(q)
            mqueries.append(mq)
            dqueries.append(dq)
        else:
            # insert a new entity
            logger.info(f"-- Entity [{etype}] doesn't exist -> [NEW ENTITY]")
            print(f"-- Entity [{etype}] doesn't exist -> [NEW ENTITY]")
            attrs += [('version', 1)]
            queries.append(insertE(etype, attrs, attr_type_dict))
    return queries, mqueries, dqueries

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




def process_record(jsobj, attr_type_dict, session): 
    queries = []
    mqueries = []
    dqueries = []
    
    q, mq, dq = processEntities(jsobj.get('entities', []), attr_type_dict, session)
    queries.extend(q)
    mqueries.extend(mq)
    dqueries.extend(dq)
    mqueries.extend(processLinks(jsobj.get('links', []), attr_type_dict, session))
    return queries, mqueries, dqueries

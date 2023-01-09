# -*- coding: utf-8 -*-
"""
Created on Wed Aug 24 15:33:34 2022

@author: imane.hafnaoui
"""
from utils.tdb_query_helpers import *

# =====
logger = logging.getLogger(__name__)


def changedAttrs(old: dict, new: dict, attr_type_dict):
    def format(k, v, atype, ref):
        if (isinstance(v, list)):
            # all([match(k, i, atype)!=ref for i in v]) TODO modify to handle lists
            return False
        elif (atype == 'datetime'):
            return datetime.strftime(parser.parse(v), '%Y-%m-%dT%H:%M:%S') != str(ref)
        else:
            return f'{re.sub(spc, " ", str(v)).lower()}' != str(ref).strip()
    return [(k, v) for k, v in new.items() if format(
        k, v, attr_type_dict[k], old.get(k, None))]


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


def insertE(etype, identifier, attrs, attr_type_dict, dict_thing_attr):
    logger.info(f"==> inserting entity {etype}")
    q = f"$_ isa {etype}"
    q += formatAttrDB(attrs, attr_type_dict)
    q += ";"
    return q


def insertR(rtype, nodes, attrs, attr_type_dict):
    logger.info(f"==> inserting relation {rtype}")
    query = "match"
    query += "".join([f" $x{i} isa {etype} \
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
        logger.info(f"? ==> Checking relation {etype} exists")
        db_ent = getEntityDB(etype, identifier, attr_type_dict, session)
        logger.debug(f"DB STATS [{etype, identifier}]: {db_ent}")
        if db_ent:
            logger.debug(f"{etype} exists! -> updating...")
            # update existing entity
            mqueries.extend(
                updateE(
                    etype,
                    identifier,
                    attrs,
                    db_ent,
                    attr_type_dict,
                    dict_thing_attrs))
        else:
            # insert a new entity
            queries.append(
                insertE(
                    etype,
                    identifier,
                    attrs,
                    attr_type_dict,
                    dict_thing_attrs))
    return queries, mqueries


def processLinks(links, attr_type_dict, session):
    mqueries = []
    # for rtype, nodes, attrs in links:
    # ids =[]
    logger.info(f"Processing Links ")
    logger.debug(f"->  {links}")
    for rtype, ids, nodes, attrs in links:
        if ids == 'check':
            # check link exists
            logger.info(f"? ==> Checking relation {rtype} exists")
            db_rel = getRelationDB(
                rtype,
                [],
                nodes,
                attr_type_dict,
                session,
                check=True)
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

    q, mq = processEntities(jsobj.get('entities', []),
                            attr_type_dict, dict_thing_attrs, session)
    queries.extend(q)
    mqueries.extend(mq)
    mqueries.extend(processLinks(jsobj.get('links', []), attr_type_dict, session))
    return queries, mqueries

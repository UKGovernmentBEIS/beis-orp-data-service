# -*- coding: utf-8 -*-
"""
Created on Fri Aug 26 21:08:18 2022

@author: imane.hafnaoui
"""

# import pandas as pd
from .utils import formatAttrDB, match
import logging
# import swifter

logger = logging.getLogger("Bulk_Migrator")

def prep_entity_insert_queries(df, attr_type_mapping):
    # format: etype(str), identifiers (list), attributes (list[(name, value)])
    def resolve_entity(x):
        # try:
            etype, _, attrs = x.tolist()
            q = f"$_ isa {etype}"
            q += formatAttrDB(attrs, attr_type_mapping)
            q += ";"
            return q
        # except Exception as e:
        #     logger.error(f"PREP ERROR: {x.tolist()}")
        #     logger.error(f"ERROR MESSAGE: {e}")

    # return df.swifter.allow_dask_on_strings(enable=True).apply(resolve_entity, axis=1).dropna().tolist()
    return df.apply(resolve_entity, axis=1).dropna().tolist()

def prep_relation_insert_queries(
    df,
    attr_type_mapping,
):
    # format: rtype (str),  nodes (list[(etype, ids(list), role)]), attributes (list[(name, value)])
    def resolve_relation(x):
        try:
            rtype, nodes, attrs = x.tolist()
            query = "match"
            query += "".join(
                [
                    f" $x{i} isa {etype} \
            {''.join([match(k,v, attr_type_mapping[k]) for k,v in eID])} ;"
                    for i, (etype, eID, _) in enumerate(nodes)
                ]
            )
            query += f"insert ({','.join([f'{role}:$x{i}' for i, (_,_,role) in enumerate(nodes)])}) isa {rtype}"
            query += formatAttrDB(attrs, attr_type_mapping)
            query += ";"
            return query
        except Exception as e:
            logger.error(f"PREP ERROR: {x.tolist()}")
            logger.error(f"ERROR MESSAGE: {e}")

    return df.apply(resolve_relation, axis=1).dropna().tolist()
    # return df.swifter.allow_dask_on_strings(enable=True).apply(resolve_relation, axis=1).dropna().tolist()


def prep_attribute_insert_queries(
    df,
    attr_type_mapping,
):

    # format: atype(str), identifier, attributes (list[(name, value)])
    def resolve_attrs(x):
        q = f'"{x.id}" isa {x.atype}'
        q += f"{' '.join([match(k, v, attr_type_mapping[k]) for k, v in x['attrs']])} ;"
        return q

    return df.apply(resolve_attrs, axis=1).dropna().tolist()

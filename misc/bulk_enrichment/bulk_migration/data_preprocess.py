import pandas as pd
from pathlib import Path
import sys, re, json
from hashlib import shake_256
from vars_orp_pbeta import *

leg_types = {
'Primary':'primaryLegislation',
'Secondary':'secondaryLegislation',
}
colmap = {'keywords':'keyword'}

STATIC_MD=['status','regulator_id','document_type', 'title']

schema = json.loads(open(SCHEMA_JSON).read())
dict_thing_attrs = {thing:v['attr'] for i in schema.values() for thing, v in i.items()}

def getElements(js, attrs):
    return [(k,v) for k,v in js.items() if k in attrs]

def hashID(obj:list) -> str :
    txt = (re.sub(r"[^A-Za-z0-9]+", "",''.join(obj).lower())).encode()
    return shake_256(txt).hexdigest(16)


def extractElements(doc):

    nodes = []
    links = []

    # insert regDoc node
    regID = [('node_id', doc.node_id)]
    nodes.append([
        "regulatoryDocument",
        regID,
        getElements(doc.to_dict(), dict_thing_attrs["regulatoryDocument"])
    ])
    # insert regulator node

    doc['regulator_id'] = doc.get('regulator_id', 'reg_default')
    reguID = [('node_id', hashID([doc.regulator_id]))]
    nodes.append([
        "regulator",
        reguID,
        getElements(doc.drop('node_id').to_dict(), dict_thing_attrs["regulator"]) + reguID
    ])

    # insert regulator agent node

    doc['user_id'] = doc.get('user_id', 'user_default')
    userID = [('node_id', hashID([doc.user_id]))]
    nodes.append([
        "regulatoryAgent",
        userID,
        getElements(doc.drop('node_id').to_dict(), dict_thing_attrs["regulatoryAgent"]) + userID
    ])

     # insert regDoc < pub > regulator <pub> reg agent
    doc['date_modified'] = doc.get('date_published', doc['date_uploaded'])
    links.append([
        'publication',
        [
            ("regulatoryDocument", regID, "issued"),
            ("regulator", reguID, "issuedBy"),
            ( "regulatoryAgent", userID, "uploader"),
        ],
        getElements(doc.to_dict(),dict_thing_attrs['publication'])
    ])

    # insert leg.org. node
    legs = doc.legislative_origins
    for leg in legs:
        leg['leg_division'] = leg.pop('division')
        leg['leg_type'] = leg.pop('type')
        leg['leg_number'] = leg.pop('number')
        leg_etype = leg_types.get(leg['leg_type'], 'legislation')
        legID = [('node_id', hashID([leg['href']]))]
        nodes.append([
            leg_etype,
            legID,
            getElements(leg, dict_thing_attrs[leg_etype]) + legID
        ])

        links =[[rtype, nds+[(leg_etype, legID, "issuedFor")], attrs] for rtype, nds, attrs in links]

     # insert regulatorAgent < partOf > regulator
    links.append([
        'partOf',
        [
            ( "regulatoryAgent", userID, "agent"),
            ("regulator", reguID, "agency"),
        ],
        getElements(doc.to_dict(),dict_thing_attrs['partOf'])
    ])

    return nodes, links



if __name__ == "__main__":

    if DATA_PATH.endswith('.p'):
        df = pd.read_pickle(DATA_PATH)
    else:
        df = pd.read_parquet(DATA_PATH)

    df = df[~df.uri.isna()]
    df = df.loc[df.raw_uri.drop_duplicates().index]
    df.rename(columns=colmap, inplace=True)
    df[['regulatory_topic','assigned_orp_topic', 'keyword']]=df[['regulatory_topic','assigned_orp_topic', 'keyword']].applymap(list)
    df['node_id'] = df.apply(lambda x: hashID(x.get(STATIC_MD).dropna().tolist()), axis=1)
    elems = df.apply(lambda x: extractElements(x.dropna()), axis=1).apply(pd.Series)
    nodes = elems[0].explode().drop_duplicates().apply(pd.Series)
    links = elems[1].explode().drop_duplicates().apply(pd.Series)

    nodes.reset_index(drop=True).to_pickle(DIR_PATH+"/nodes.p")
    links.reset_index(drop=True).to_pickle(DIR_PATH+"/links.p")

    # attrs
    aoa_data = [
        ('regulator_id','data/misc/regulator_list.parquet'),
        ('assigned_orp_topic', 'data/misc/topic_id_mapping.parquet')
    ]
    attrdf = pd.DataFrame()
    for atype, fn in aoa_data:
        alist = pd.read_parquet(fn)
        adf=alist.apply(lambda x: (atype, x[atype], getElements(x.to_dict(), dict_thing_attrs[atype])), axis=1).apply(pd.Series)
        attrdf = pd.concat([attrdf, adf])

    attrdf.columns = ['atype', 'id', 'attrs']
    attrdf.to_pickle(DIR_PATH+"/attributes.p")

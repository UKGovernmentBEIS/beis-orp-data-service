from helpers import *
from pandas import DataFrame, Timestamp

return_vals = ['title', 'summary', 'document_uid', 'regulator_id', "regulatory_topic",
               'document_type', 'keyword', 'uri', 'status', 'language', 'document_format',
               'date_published', 'date_uploaded', 'legislative_origins', 'version']
leg_vals = ['href', 'title', 'leg_type', 'leg_division']

def query_builder(event):
    event = {k: clean_text(v) for k, v in event.items()}
    ### Build TQL query from search params

    # Document API
    if event.get('id'):
        query = 'match $x isa regulatoryDocument, has attribute $attribute'
        query += f', has document_uid "{event["id"].lower()}"'
        query += ';not {$x has status "archive";};  group $x;'
        return query

    # Related reg docs (links to legislation search)
    elif event.get('legislation_href'):
        query = 'match $x isa legislation, has URI $id; $id like "'
        query += '|'.join([leg for leg in event.get('legislation_href', [])])
        query += '''";  $regdoc isa regulatoryDocument, has attribute $attribute;
         not {$regdoc has status "archive";};
            (issuedFor:$x,issued:$regdoc) isa publication; limit 1000;
            group $x;'''
        return query

    # Search API
    else:
        subq = ""
        query = 'match $x isa regulatoryDocument, has document_uid $uid, has date_published $dt'
        # simple filters
        if event.get('regulatory_topic'):
            query += f', has regulatory_topic "{event["regulatory_topic"]}"'

        # list filters [AND]
        if event.get('keyword'):
            query += ''.join(
                [f', has keyword "{get_lemma(kw.strip().lower())}"' for kw in event['keyword'].split(' ')])

        # list filters [OR]
        paramOR = {'document_type', 'regulator_id', 'status'}
        for param in set(event.keys()) & paramOR:
            query += f', has {param} ${param}'
            subq += f"; ${param} like \"{'|'.join([i for i in event[param]])}\""

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

        query += subq
        query += ';not {$x has status "archive";}; get $uid, $dt, $x; limit 10000; group $x;'
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
                rd['regulatory_topic'] = max(aot, key=lambda x: len(
                    x.split('/'))) if isinstance(aot, list) else aot
            data.append(get_select_dict(rd, return_vals))
        doc['related_docs'] = data
        docs.append(doc)
    return docs


def format_doc_results(ans, session, page=0, page_size=10, id_search=False, asc=False):
    
    def get_docs_attrs(uid_list):
        # Query the graph database for document attributes 
        query = 'match $x isa regulatoryDocument, has document_uid $id, has attribute $a; $id like "'
        query += '|'.join(uid_list)
        query += '"; group $x;'
        ans = matchquery(query, session)
        res = DataFrame([dict(getUniqueResult(a.concept_maps()))
                                            for a in ans])
        return res.sort_values('date_published', ascending=asc)

    def get_docs_legs(uid_list):
        # Query the graph database for legislative origins

        query = 'match $x isa regulatoryDocument, has document_uid $id; $id like "'
        query += '|'.join(uid_list)
        query += '''";  $leg isa legislation, has attribute $attribute;
            (issuedFor:$leg,issued:$x) isa publication;
            group $x;'''
        ans = matchquery(query=query, session=session)
        legs = DataFrame(
            group_of_group(
                ans,
                grouping='leg').items(),
            columns=[
                'document_uid',
                'legislative_origins'])
        return legs
    
    res = DataFrame([dict(getUniqueResult(a.concept_maps()))
                        for a in ans])
    if not id_search: 
        res = res.dropna(subset=['document_uid']).sort_values('date_published', ascending=asc)
        res = res.iloc[page:page + page_size]
        res = get_docs_attrs(res.document_uid)        
    
    legs = get_docs_legs(res.document_uid.tolist())

    # Merging leg.orgs info with reg document
    df = res.merge(legs, on='document_uid', how='left')
    legmap = {'leg_type': 'type', 'leg_division': 'division'}
    df.legislative_origins = df.legislative_origins.fillna("").apply(list).apply(lambda x: list(
        filter(None, [remap(get_select_dict(a, leg_vals), legmap) for a in x])))

    # get noun for keywords
    df.keyword = df.keyword.apply(lambda x: list(
        set([lemma2noun(kw) for kw in x]) if type(x)==list else []))

    # get assigned topic
    if 'assigned_orp_topic' in df.columns:
        df.regulatory_topic = df.assigned_orp_topic.apply(lambda aot: max(
            aot, key=lambda x: len(x.split('/'))) if isinstance(aot, list) else aot)

    df = df.applymap(lambda x: format_datetime(x) if isinstance(x, Timestamp) else x)
    return df.fillna('').apply(
        lambda x: get_select_dict(x, return_vals), axis=1).tolist()


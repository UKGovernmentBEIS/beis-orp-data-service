# -*- coding: utf-8 -*-


from pathlib import Path

from pdf_to_text.pdf_to_text import pdf_converter
from odf_to_text.odf_to_text import odf_converter
from docx_to_text.docx_to_text import docx_converter
from date_generation.date_generation import date_generation
from title_generation.title_generation import title_generator
from summarisation.summarisation import summariser
from keyword_extraction.keyword_extraction import keyword_extraction
# from legislative_origin.lo_extraction import lo_extraction
from datetime import datetime
import pandas as pd
import json, os

md_keys = ['title', 'summary', 'document_uid', 'uri', 'data', 'subject_keywords', 'status', 'version']
WORKDIR='/Users/imane.hafnaoui/gitpod/tmp/bulk_data_md_ext/data/pdf/'
TXTDIR='data/txt/'
MDDIR='data/md/'
SVPATH = f'bulk_data_enriched_{datetime.now().strftime("%Y-%m-%d")}.p'

date_uploaded = datetime.now().isoformat()
format_mapper = {
    'pdf': pdf_converter,
    'odf': odf_converter,
    'docx': docx_converter,
    'doc': docx_converter
}


def main(path):
    df = []
    flist = list(Path(path).glob("*"))
    for i, filename in enumerate(flist):
        try:
            fn = str(filename)
            print(f"=== {i}/{len(flist)} -> {fn}")
            docuid, ftype = filename.name.split('.')
            if os.path.exists(f'data/md/{docuid}.json'):
                print(f'+++ Skipping {fn}')
                continue

            text, title, datepub = format_mapper[ftype](fn, docuid, TXTDIR)
            # print('date_generation')
            datepub = date_generation(text, datepub)
            # print('title_generator')
            title = title_generator(text, title)
            # print('summariser')
            summary = summariser(text)
            # print('LO')
            # los = lo_extraction(text)
            # print('keywords')
            keywords = keyword_extraction(text)
            # print('packaging...')
            data = {
                'dates': {
                    'date_published': datepub,
                    'date_uploaded' : date_uploaded
                },
                'legislative_origins': []#los
            }
            mdata = dict(zip(md_keys,
            [
                title, summary, docuid, filename.name, data, keywords, 'published', '1'
            ]))
            df.append(mdata)
            json.dump(mdata,open(f'{MDDIR}/{docuid}.json','w', encoding='utf-8'), ensure_ascii=False, indent=4)
        except:
            continue
    return pd.DataFrame(df)

df = main(WORKDIR)        
df.to_pickle(SVPATH)

# # ===postproc

# from pathlib import Path
# fn='/Users/imane.hafnaoui/gitpod/MXTrepos/beis-orp-data-service/bulk_enrichment/data/md'
# flist = Path(fn).glob("*.json")
# df = pd.DataFrame([pd.read_json(p, typ="series") for p in flist])
# fnn='/Users/imane.hafnaoui/gitpod/tmp/bulk_data_md_ext/data/metadata/'
# flist = Path(fnn).glob("*.json")
# dff = pd.DataFrame([pd.read_json(p, typ="series") for p in flist])
# d=df.merge(dff[['document_uid', 'regulator_id']], on='document_uid', how='left')

# def fct(x, uid):
#     leg = dff[dff.document_uid==uid].data.iloc[0]['legislative_origins']
#     x.data['legislative_origins'] = leg
#     return x
# d=d.apply(lambda x: fct(x, x.document_uid), axis=1)
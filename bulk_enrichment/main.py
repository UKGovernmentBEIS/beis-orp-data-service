# -*- coding: utf-8 -*-


from pathlib import Path

from pdf_to_text.pdf_to_text import pdf_converter
from odf_to_text.odf_to_text import odf_converter
from docx_to_text.docx_to_text import docx_converter
from date_generation.date_generation import date_generation
from title_generation.title_generation import title_generator
from summarisation.summarisation import summariser
from keyword_extraction.keyword_extraction import keyword_extraction
from legislative_origin.lo_extraction import lo_extraction
from datetime import datetime
import pandas as pd

md_keys = ['title', 'summary', 'document_uid', 'uri', 'data', 'subject_keywords', 'status', 'version']
WORKDIR='test_set/'
TXTDIR='data/txt/'
SVPATH = f'bulk_data_enriched_{datetime.now().strftime("%Y-%M-%D")}.p'

date_uploaded = datetime.now().isoformat()
format_mapper = {
    'pdf': pdf_converter,
    'odf': odf_converter,
    'docx': docx_converter,
    'doc': docx_converter
}

df = []
def main(path):
    flist = list(Path(path))#.glob("*.pdf"))
    for i, filename in enumerate(flist):
        fn = str(filename)
        print(f"{i}/{len(flist)} -> {fn}")
        docuid, ftype = filename.name.split('.')
        text, title, datepub = format_mapper[ftype](fn, docuid, TXTDIR)
        datepub = date_generation(text, datepub)
        title = title_generator(text, title)
        summary = summariser(text)
        los = lo_extraction(text)
        keywords = keyword_extraction(text)

        data = {
            'dates': {
                'date_published': datepub,
                'date_uploaded' : date_uploaded
            },
            'legislative_origins': los
        }
        mdata = dict(zip(md_keys,
        [
            title, summary, docuid, filename.name, data, keywords, 'published', '1'
        ]))
        df.append(mdata)
    return pd.DataFrame(df)
        
pd.to_pickle(main(WORKDIR), SVPATH)
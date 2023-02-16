# -*- coding: utf-8 -*-
"""
Created on Mon Mar 3 10:48:33 2022

@author: Imane.Hafnaoui


Detects legislation origin by searching through a lookup table of existing acts. We do this through exact matching. 
The matcher goes through two stages:
    Stage 1:
        - Narrows down the search space to candidate legislation in the document text by year of publication;
        - Segment text into sentences.Then;
    Stage 2:
        - Runs exact matching against the entries in the table per sentence and stop when legislation is mentionned.

    
"""
from spacy.matcher import  Matcher, PhraseMatcher 
# from spaczz.matcher import FuzzyMatcher
# from database.db_connection import get_hrefs, get_canonical_leg

# User-defined params
keys = ['detected_ref', 'start', 'end']

# EXACT MATCHING

def exact_matcher(title, docobj, nlp):
    """
    Detects legislation in body of judgement by searching for the exact match of the title in the text.

    Parameters
    ----------
    title : string
        Title of a legislation.
    docobj : spacy.Doc
        The body of the judgement.
    nlp : spacy.English
        English NLP module.

    Returns
    -------
    matched_text : list(tuple)
        List of tuples of the form ('detected reference', 'start position', 'end position', 100)

    """
    phrase_matcher = PhraseMatcher(nlp.vocab)
    phrase_list = [nlp(title)]
    phrase_matcher.add("Text Extractor", None, *phrase_list)

    matched_items = phrase_matcher(docobj)

    matched_text = []
    for _, start, end in matched_items:
        span = docobj[start: end]
        matched_text.append((span.text, start, end, 100))
    return matched_text


def lookup_pipe(titles, docobj, nlp, method):
    """
    Executes the 'method' matcher againt the judgement body to detect legislations.

    Parameters
    ----------
    titles : list(string)
        List of legislation titles.
    docobj : spacy.Doc
        The body of the judgement.
    nlp : spacy.English
        English NLP module.
    method : function
        Function specifying which matcher to execute (fuzzy or exact).
    conn : database connection
        Database connection to the legislation look-up table.
    Returns
    -------
    results : list(dict)
        List of dictionaries of the form {
            'detected_ref'(string): 'detected reference in the judgement body', 
            'ref'(string): 'matched legislation title', 
            'canonical'(string): 'canonical form of legislation act',
            'start'(int): 'start position of reference',
            'end'(int): 'end positin of reference',
            'confidence'(int): 'matching similarity between detected_ref and ref'}

    """
    results = {}
    # for every legislation title in the table
    for title in nlp.pipe(titles):
        # detect legislation in the judgement body
        matches = method(title.text, docobj, nlp)
        if matches:
            results[title.text] = results.get(title.text, []) + matches
    return results


def detect_year_span(docobj, nlp):
    """
    Detects year -like text in the judgement body.

    Parameters
    ----------
    docobj : spacy.Doc
        The body of the judgement.
    nlp : spacy.English
        English NLP module.

    Returns
    -------
    dates : list[string]
        List of year -like strings.

    """
    pattern = [{"SHAPE": "dddd"}]
    dmatcher = Matcher(nlp.vocab)
    dmatcher.add('date matcher', [pattern])
    dm = dmatcher(docobj)
    dates = [docobj[start:end].text for _, start, end in dm]
    dates = set([int(d) for d in dates if (len(d) == 4) & (d.isdigit())])
    return dates
######

def leg_pipeline(leg_titles, nlp, docobj):
    dates = detect_year_span(docobj, nlp)
    # filter the legislation list down to the years detected above
    sents = list(docobj.sents)
    for sent in sents:
        sdates = [year  for year in dates if str(year) in sent.text]
        titles = leg_titles[leg_titles.year.isin(sdates)]
        relevant_titles = titles.candidate_titles.drop_duplicates().tolist()
        # print(f'\t Looking through {len(relevant_titles)} possible candidates...')
        results = lookup_pipe(relevant_titles, docobj, nlp,
                            exact_matcher)
        if results:
            break

    results = dict([(k, [dict(zip(keys, j)) for j in v])
                    for k, v in results.items()])

    refs = set(results.keys())
    print(f"\t Found [{len(refs)}] legislative origins.")
    return refs

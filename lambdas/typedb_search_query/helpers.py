from typedb.client import TransactionType
from datetime import datetime
from pandas import DataFrame
from word_forms_loc.word_forms_loc import get_word_forms
from word_forms_loc.lemmatizer import lemmatize

# from word_forms.word_forms import get_word_forms
# from word_forms.lemmatizer import lemmatize

import re
sp_chars = r'"|,|;'

def get_lemma(word):
    # return word
    try:
        return lemmatize(word)
    except ValueError as err:
        if 'is not a real word' in err.args[0]:
            return word
        else:
            raise ValueError(err)


def lemma2noun(lemma):
    # return lemma
    nn = list(get_word_forms(lemma).get('n', []))
    return sorted(nn, key=len)[0] if nn else lemma


def clean_text(text):
    if isinstance(text, list):
        return [clean_text(i) for i in text]
    elif isinstance(text, str):
        return re.sub(sp_chars, '', text).encode("ascii", "ignore").decode()
    else:
        return text


def format_datetime(date): return datetime.isoformat(date)


def get_select_dict(results: dict, selc: list): return {k: (format_datetime(
    v) if isinstance(v, datetime) else v)for k, v in results.items() if (v != '') & (k in selc)}


def remap(d: dict, mapd: dict): return {
    mapd.get(k, k): v for k, v in d.items()}


def group_attributes(attr):
    return DataFrame(attr).drop_duplicates().groupby(0)[1].apply(list).apply(
        lambda x: x[0] if len(x)==1 else x
        ).to_dict()


def getUniqueResult(results):
    res = [(i.get_type().get_label().name(), i.get_value())
           for a in results for i in a.concepts() if i.is_attribute()]
    return group_attributes(res)


def group_of_group(results, id='id', grouping='y', attribute='attribute'):
    ret = {}
    for res in results:
        gp1 = res.concept_maps()[0].map()[id].get_value()
        attrs = [i.map() for i in res.concept_maps()]
        df = DataFrame([(i[grouping].get_iid(),
                            (i[attribute].get_type().get_label().name(),
                             i[attribute].get_value())) for i in attrs])
        ret[gp1] = df.groupby(0)[1].apply(list).apply(group_attributes).to_list()
    return ret


def matchquery(query, session, group=True):
    with session.transaction(TransactionType.READ) as transaction:
        print("Query:\n %s" % query)
        iterator = transaction.query().match_group(
            query) if group else transaction.query().match(query)
        results = [ans for ans in iterator]
        return results
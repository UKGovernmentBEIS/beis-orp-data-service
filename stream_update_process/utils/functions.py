from hashlib import shake_256
import re


def hashID(obj:list) -> str :
    print(obj)
    txt = (re.sub(r"[^A-Za-z0-9]+", "",''.join(obj).lower())).encode()
    return shake_256(txt).hexdigest(16)


def getElements(js, attrs):
    return [(k,v) for k,v in js.items() if k in attrs]

def key_remapper(dictionary, mapper):
    return {mapper.get(k, k): v for k, v in dictionary.items()}
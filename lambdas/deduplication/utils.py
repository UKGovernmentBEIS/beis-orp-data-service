import kshingle as ks
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from datasketch import MinHash, MinHashLSH


lsh = MinHashLSH(threshold=0.9)
stop_words = set(stopwords.words('english'))


# Preprocess text before shingling
def preprocess(text):
    text = text.lower()
    word_tokens = word_tokenize(text)
    filtered_sentence = " ".join([w for w in word_tokens if not w in stop_words])
    return filtered_sentence
    
    
def getHash(doc, k=5):
    # Generate shingle sets for each document as documents are lengthy

    shingles = set(ks.shingleset_k(preprocess(doc), k))
    hash = MinHash(num_perm=256, seed=1)

    for s in shingles:
        hash.update(s.encode('utf8'))

    return hash.hashvalue


def create_hash_list(text):
    """
    param: text: Str
    returns: hash_list: list of hashes
    """
    hash_np = getHash(text)
    hash_list = hash_np.tolist()
    return hash_np, hash_list

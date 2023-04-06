import kshingle as ks
from nltk.tokenize import word_tokenize
from datasketch import MinHash, MinHashLSH


lsh = MinHashLSH()
from pyspark import SparkFiles

stopwords_path=SparkFiles.get('resources/stopwords.txt')
stopwords = set(open(stopwords_path, 'r').read().split('\n'))

def preprocess(text):
    text = text.lower()
    word_tokens = word_tokenize(text)
    filtered_sentence = " ".join([w for w in word_tokens if w not in stopwords])
    return filtered_sentence

def getHash(doc, k=5):
    # Generate shingle sets for each document as documents are lengthy

    shingles = set(ks.shingleset_k(preprocess(doc), k))
    hash = MinHash(num_perm=256, seed=1)

    for s in shingles:
        hash.update(s.encode('utf8'))

    return hash.hashvalues

def create_hash(text):
    """
    param: text: Str
    returns: hash_list: list of hashes
    """
    hash_np = getHash(text)
    hash_list = hash_np.tolist()
    return '_'.join(map(str, hash_list))
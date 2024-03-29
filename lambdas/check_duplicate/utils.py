import kshingle as ks
from nltk.tokenize import word_tokenize
from datasketch import MinHash, MinHashLSH

lsh = MinHashLSH()

stopwords = open('./stopwords.txt', 'r')
stopwords = stopwords.read()
stopwords = [i for i in stopwords.split('\n')]
stopwords = set(stopwords)


def preprocess(text):
    '''Preprocess text before shingling
    Lower, tokenize and remove stop words'''
    text = text.lower()
    word_tokens = word_tokenize(text)
    filtered_sentence = ' '.join([w for w in word_tokens if w not in stopwords])
    return filtered_sentence


def getHash(doc, k=5):
    '''Generate shingle sets for each document as documents are lengthy'''
    shingles = set(ks.shingleset_k(preprocess(text=doc), k))
    hash = MinHash(num_perm=256, seed=1)

    for s in shingles:
        hash.update(s.encode('utf8'))

    return hash.hashvalues


def create_hash_list(text):
    '''
    param: text: Str
    returns: hash_list: list of hashes
    '''
    hash_np = getHash(text)
    hash_list = list(map(str, hash_np.tolist()))
    return hash_np, hash_list

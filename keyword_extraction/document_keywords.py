import openpyxl
import pandas as pd
import re
import os
from sklearn.feature_extraction.text import CountVectorizer
import wordninja
from keybert import KeyBERT
from bs4 import BeautifulSoup
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
wnl = WordNetLemmatizer()

# Define stopwords
stop_words = stopwords.words('english')
english_stop_words = [w for w in ENGLISH_STOP_WORDS]
stop_words.extend(["use", "uses", "used", "www", "gov",
                  "uk", "guidance", "pubns", "page"])
stop_words.extend(english_stop_words)

# Preprocess data after embeddings are created


def pre_process_tokenization_function(text):
    text = BeautifulSoup(text).get_text()
    # fetch alphabetic characters
    text = re.sub("[^a-zA-Z]", " ", text)
    # define stopwords
    remove_stop_words = set(stop_words)
    # lowercase
    text = text.lower()
    # tokenize
    word_tokens = word_tokenize(text)
    filtered_sentence = []
    for w in word_tokens:
        if w not in remove_stop_words:
            filtered_sentence.append(w)
    # # Remove any small characters remaining
    filtered_sentence = [word for word in filtered_sentence if len(word) > 1]
    # # Lemmatise text
    lemmatised_sentence = [wnl.lemmatize(word) for word in filtered_sentence]
    return lemmatised_sentence


# Import KeyBERT
kw_model = KeyBERT()

# Vectorizer model
# prevents noise and improves representation of clusters
vectorizer_model = CountVectorizer(
    stop_words="english",
    tokenizer=pre_process_tokenization_function)

keywords_list = []
txt_list = []

# Read and extract keywords for uuid docs
for txt in os.listdir("/Users/thomas/Documents/BEIS/input_data/new_txt/"):
    file = open("/Users/thomas/Documents/BEIS/input_data/new_txt/" + txt, "r")
    X = file.read()

    # Extract keywords
    d = re.sub("Health and Safety Executive", "", X)
    d = re.sub("Ofgem", "", d)
    d = re.sub("Environmental Agency", "", d)
    d = " ".join(wordninja.split(d))
    keywords = kw_model.extract_keywords(d, vectorizer=vectorizer_model, top_n=10)
    keywords_list.append(keywords)
    txt_list.append(txt)


df = pd.DataFrame({"UUID": txt_list, "Keywords": keywords_list})
df.to_excel("20221210-Keywords.xlsx", engine="openpyxl")

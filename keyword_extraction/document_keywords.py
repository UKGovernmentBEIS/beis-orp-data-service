from nltk.stem import WordNetLemmatizer
wnl = WordNetLemmatizer()
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
from nltk.tokenize import word_tokenize
from bs4 import BeautifulSoup
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
import re

# Stop words
stop_words = stopwords.words('english')
english_stop_words = [w for w in ENGLISH_STOP_WORDS]
regulator_names_list = ["hse", "ofgem", "ea"]

stop_words.extend(["use", "uses", "used", "www", "gov", "uk", "guidance", "pubns"])
stop_words.extend(regulator_names_list)
stop_words.extend(english_stop_words)

# define stopwords
remove_stop_words = set(stop_words)

# Preprocess data after embeddings are created
def pre_process_tokenization_function(text):
    text = BeautifulSoup(text).get_text()
    # fetch alphabetic characters
    text = re.sub("[^a-zA-Z]", " ", text)
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

# Define vectorization model
vectorizer = TfidfVectorizer(analyzer='word',stop_words= stop_words, max_features=15, tokenizer = pre_process_tokenization_function)

# Get keywords from document
def get_keywords(text):
    vectorizer.fit([" ".join(pre_process_tokenization_function(text))])
    keywords = vectorizer.get_feature_names_out()
    return keywords


import os
import pandas as pd
import openpyxl

keywords_list = []
title_list = []

DIRECTORY = "/Users/thomas/Documents/BEIS/input_data/all_pdfs_text/"
for title in os.listdir(DIRECTORY):
    fileObject = open(DIRECTORY + title, "r")
    guidance = fileObject.read()
    keywords = get_keywords(guidance)
    keywords_list.append(keywords)
    title_list.append(title)

pd.DataFrame({"Title" : title_list, "Keywords" : keywords_list}).to_excel("keywords.xlsx", engine = "openpyxl")
from nltk.tokenize import word_tokenize
from bs4 import BeautifulSoup
import re
from nltk.stem import WordNetLemmatizer
wnl = WordNetLemmatizer()


# Define tokenization function
def pre_process_tokenization_function(
        documents: str,
        stop_words,
        wnl):

    # Preprocess data after embeddings are created
    text = BeautifulSoup(documents).get_text()
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
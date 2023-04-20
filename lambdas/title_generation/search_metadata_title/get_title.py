import re
import spacy
from tqdm import tqdm
from typing import List
from preprocess.preprocess_functions import removing_regulator_names

my_pattern = re.compile(r'\s+')


# Shorten text for input to title extraction model
def percentage_shortener(text: str, percentage=0.1) -> str:
    """
    param: text: Str document text
    param: percentage: float percentage of document to sample
    returns: shortened_complete: shortened text
        Shorten text for iteration of title over candidate titles
    """
    text = removing_regulator_names(text)
    length = int(len(text) * percentage)
    shortened = " ".join(text.split(" ")[: length])
    shortened_complete = shortened + text.replace(shortened, "").split(".")[0]
    return shortened_complete


def rolling_padded_sentence(metadata_title: str, text: str, padding=0) -> List:
    """
    param: metadata_title: Str title
    param: text: Str document text
    param: padding: int padding around candidate titles
    returns: candidate_titles: List of titles to iterate the metadata title over
    """
    text = percentage_shortener(text)
    candidate_titles = []
    padded_title_length = len(metadata_title.split(" ")) + padding
    tokenized_text = text.split(" ")

    for starting_idx in range(0, len(tokenized_text) - padded_title_length + 1):
        candidate_title = tokenized_text[starting_idx: starting_idx +
                                         padded_title_length]
        candidate_titles.append(" ".join(candidate_title))

    # Capping the candidate title list at 1000
    if len(candidate_titles) > 1000:
        return candidate_titles[0: 1000]

    else:
        return candidate_titles


# Define function to get similarity scores
def get_similarity_scores(title: str, candidate_titles: List) -> float:
    """
    param: title: Str title
    param: candidate_titles: List of candidate titles
    returns: score: highest similarity score of metadata title over list of candidate titles
        Makes use of a pretrained model to compare embeddings of the title and candidate title
    """
    similarity_scores = []

    nlp = spacy.load("en_core_web_lg")

    title = nlp(re.sub(r'[^\w\s]', '', title.lower()))

    for sent in tqdm(candidate_titles):
        score = title.similarity(nlp(re.sub(r'[^\w\s]', '', sent.lower())))
        similarity_scores.append(score * 100)

    # Get score of match
    if len(similarity_scores)>0:
        score = max(similarity_scores)
    else:
        score = 0

    return score


def identify_metadata_title_in_text(metadata_title: str, text: str) -> float:
    """
    param: metadata_title: Str title
    param: text: Str document text
    returns: score: float highest score
        Function that brings all predefined functions together
    """

    candidate_titles = rolling_padded_sentence(metadata_title=metadata_title, text=text)
    score = get_similarity_scores(metadata_title, candidate_titles)

    return score

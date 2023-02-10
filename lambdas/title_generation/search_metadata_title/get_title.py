import numpy as np
from tqdm import tqdm
from numpy.linalg import norm
from sentence_transformers import SentenceTransformer


similarity_model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')


# Shorten text for input to title extraction model
def percentage_shortener(text, percentage = 0.1):
    length = int(len(text)*percentage)
    shortened = " ".join(text.split(" ")[ : length])
    shortened_complete = shortened + text.replace(shortened, "").split(".")[0]
    return shortened_complete


def rolling_padded_sentence(metadata_title, text, padding = 2):

    text = percentage_shortener(text)
    candidate_titles = []
    padded_title_length = len(metadata_title.split(" ")) + padding
    tokenized_text = text.split(" ")

    for starting_idx in range(0, len(tokenized_text) - padded_title_length + 1):
        candidate_title = tokenized_text[starting_idx : starting_idx + padded_title_length]
        candidate_titles.append(" ".join(candidate_title))

    if len(candidate_titles) > 1000:
        return candidate_titles[0 : 1000]

    else:
        return candidate_titles


# Define function to get similarity scores
def get_similarity_scores(title, candidate_titles):
    similarity_scores = []

    for sent in tqdm(candidate_titles):
        embeddings = similarity_model.encode( [ title.lower(), sent.lower() ] )

        # compute cosine similarity
        cosine = np.dot(embeddings[0],embeddings[1])/(norm(embeddings[0])*norm(embeddings[1]))  
        similarity_scores.append(cosine*100)

    # Index of max(similarity_score)
    max_idx = similarity_scores.index(max(similarity_scores))

    # Get score of match
    score = max(similarity_scores)

    return max_idx, score


def refine_returned_title(max_idx, title, candidate_titles):

    # Where title is at the start
    if max_idx == 0:
        longer_candidate_title = candidate_titles[max_idx].split(" ") + [candidate_titles[max_idx + 1].split(" ")[-1]]

    # Where title is at the end
    elif max_idx == len(candidate_titles) - 1:
        longer_candidate_title = [candidate_titles[max_idx - 1].split(" ")[-1] ]+ candidate_titles[max_idx].split(" ")

    # Where title is in the middle
    else:
        longer_candidate_title = [candidate_titles[max_idx - 1].split(" ")[-1]] + candidate_titles[max_idx].split(" ") + [candidate_titles[max_idx + 1].split(" ")[-1]]

    length_of_longer_candidate_title = len(longer_candidate_title)

    candidate_title_permutations = []

    for starting_idx, word in enumerate(longer_candidate_title):
        for idx in range(starting_idx + 1, length_of_longer_candidate_title - starting_idx):
            candidate_title = longer_candidate_title[starting_idx : idx]
            candidate_title_permutations.append(" ".join(candidate_title))

    max_idx, score = get_similarity_scores(title.lower(), [t.lower() for t in candidate_title_permutations])

    returned_title = candidate_title_permutations[max_idx]

    return returned_title, score


def identify_metadata_title_in_text(metadata_title, text):

    candidate_titles = rolling_padded_sentence(metadata_title = metadata_title, text = text)

    initial_candidate_title_idx, initial_candidate_title_score = get_similarity_scores(metadata_title, candidate_titles)

    title, score = refine_returned_title(initial_candidate_title_idx, metadata_title, candidate_titles)

    return title, score



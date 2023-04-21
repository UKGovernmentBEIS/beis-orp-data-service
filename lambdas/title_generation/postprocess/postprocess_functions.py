from typing import List
from nltk.corpus import stopwords
import re

def delete_repeated_ngrams(text_list: List) -> List:
    """
    param: title: Str
    returns: title: Str without punctuation at the end
        Cleaned title with repeated words that aren't stopwords removed
    """
    cleaned_list = []
    for word in text_list:
        if word in stopwords.words("english"):
            cleaned_list.append(word)
            continue
        elif word in cleaned_list:
            continue
        else:
            cleaned_list.append(word)
    return cleaned_list


def find_nth(title: str, char: str, n: int) -> int:
    """
    param: title: Str
    returns: start: int of index where the char is located
        Find where the character is located in the sentence
    """
    start = title.find(char)
    while start >= 0 and n > 1:
        start = title.find(char, start + len(char))
        n -= 1
    return start


def remove_open_brackets(title: str) -> str:
    """
    param: title: Str
    returns: title: Str without an open bracket near the end of the title
        Remove open brackets from titles
    """
    open_bracket_counter = 0
    close_bracker_counter = 0
    for char in title:
        if char == "(":
            open_bracket_counter += 1
        elif char == ")":
            close_bracker_counter += 1
    if open_bracket_counter > close_bracker_counter:
        end_idx = find_nth(title, "(", open_bracket_counter)
        title = title[: end_idx]
        return title
    else:
        return title


def remove_trailing_stopwords_and_single_chars(text_list: List) -> List:
    """
    param: text_list: List of words in title
    returns: text_list: List of words in title
        Removes stopwords and single characters at the end of the generated title
    """
    while text_list[-1].lower() in stopwords.words("english") or (len(text_list[-1])
                                                                  == 1 and text_list[-1].isdigit()
                                                                  is False and text_list[-1].lower() != "a"):
        text_list = text_list[:-1]
    return text_list


def remove_other_patterns(title: str) -> str:
    """
    param: title: Str
    returns: title: Str
        Title without set patterns
        Pattern 1: page %d of %d
    """    
    pattern = re.compile(r'page (\d+) of (\d+)')
    match = pattern.search(title)
    if match:
        return title[:match.start()] + title[match.end():]
    else:
        return title

def remove_table_of_contents(title: str):
    """
    Checks if Contents or Table of Contents is followed by 1, and then 
    whether the number 2 follows the subsequent sequence of words
    params: title
        returns: title
    """
    pattern = r'\b(Contents|Table of Contents|Table of contents)\b\s*1\s+\D+?\s+2'
    match = re.search(pattern, title)
    if match:
        return title[:match.start()].strip()
    else:
        return title
    
def postprocess_title(title: str) -> str:
    """
    param: title: Str
    returns: title: Str
        Fully processes generated title and capital cases
    """
    title = remove_other_patterns(title)
    title = remove_open_brackets(title)
    text_list = delete_repeated_ngrams(title.strip().split(" "))
    text_list = remove_trailing_stopwords_and_single_chars(text_list)

    # Capital case
    returned_list = []
    for idx, word in enumerate(text_list):
        if idx == 0 or (
                word.isupper() is False and word not in stopwords.words("english")):
            returned_list.append(word.title())
        else:
            returned_list.append(word)

    title = remove_table_of_contents(" ".join(returned_list))
    return title

import string
import nltk
nltk.download('punkt')
from nltk.corpus import stopwords

# REMOVE PUNCTUATION AT THE END OF TITLE
def remove_punc_at_end(title):
    if title[-1] in string.punctuation.replace(")", "").replace("\"", ""):
        title = title[:-1]
    return title

def delete_repeated_ngrams(text_list):
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

# Remove anything after open bracket
def find_nth(title, char, n):
    start = title.find(char)
    while start >= 0 and n > 1:
        start = title.find(char, start+len(char))
        n -= 1
    return start

def remove_open_brackets(title):
    open_bracket_counter = 0
    close_bracker_counter = 0
    for char in title:
        if char == "(":
            open_bracket_counter +=1
        elif char == ")":
            close_bracker_counter +=1
    if open_bracket_counter > close_bracker_counter:
        end_idx = find_nth(title, "(", open_bracket_counter)
        title = title[ : end_idx]
        return title
    else:
        return title


# IF IT IS STOPWORD AT END OF SENTENCE......
def remove_trailing_stopwords_and_single_chars(text_list):
    while text_list[-1].lower() in stopwords.words("english") or (len(text_list[-1]) == 1 and text_list[-1].isdigit() == False and text_list[-1].lower() != "a"):
        text_list = text_list[:-1]
    return text_list


def postprocess_title(title):
    title = remove_open_brackets(title)
    # title = remove_punc_at_end(title)
    text_list = delete_repeated_ngrams(title.strip().split(" "))
    text_list = remove_trailing_stopwords_and_single_chars(text_list)
    # Capital case
    returned_list = []
    for idx, word in enumerate(text_list):
        if idx == 0 or (word.isupper() == False and word not in stopwords.words("english")):
            returned_list.append(word.title())
        else:
            returned_list.append(word)
    return " ".join(returned_list)
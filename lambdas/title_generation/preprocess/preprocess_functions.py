import wordninja
import string


with open("../list_of_regulators_uk.txt", "r") as f:
    fileObject = f.read()
    regulator_name_list = fileObject.split("\n")


def removing_regulator_names(text, regulator_name_list):
    for reg in regulator_name_list:
        text = text.replace(reg, "")
    return text


# Often text is spaced out at the top of documents
def delete_single_characters(text):
    # If first 5 tokens are == len(1) it is a sign the text is malformed
    short_char_counter = 0
    for char in text.strip().split(" ")[:5]:
        if len(char) == 1:
            short_char_counter +=1
    if short_char_counter == 5:        
        text = wordninja.split("".join(text).replace(" ", ""))
        return " ".join(text)
    else:
        return text


# Preprocess text coming in
def remove_excess_punctuation(text):
    # Clean punctuation spacing
    text = text.replace(" .", "")
    for punc in string.punctuation:
        text = text.replace(punc + punc, "")
    return text


def preprocess(text):
    text = removing_regulator_names(text, regulator_name_list)
    text = delete_single_characters(text)
    text = remove_excess_punctuation(text)
    return text
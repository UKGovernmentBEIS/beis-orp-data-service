import string
import wordninja


with open("preprocess/regulator_name_list.txt", "r") as f:
    fileObject = f.read()
    regulator_name_list = fileObject.split("\n")
    regulator_name_list = ["Logo of the " +
                           i for i in regulator_name_list] + regulator_name_list


def removing_regulator_names(text: str, regulator_name_list=regulator_name_list) -> str:
    """
    param: text: Str document text
    param: regulator_name_list: List list of regulator names to remove from text
    returns: text: Str cleaned document text
        Removal of regulator names from text to clean the text before the title is predicted
    """
    for reg in regulator_name_list:
        text = text.replace(reg, "")

    return text


def delete_single_characters(text) -> str:
    """
    param: text: Str document text
    returns: text: Str cleaned document text
        Removal of single characters at the start of text
    """
    # If first 5 tokens are == len(1) it is a sign the text is malformed
    short_char_counter = 0
    for char in text.strip().split(" ")[:5]:
        if len(char) == 1:
            short_char_counter += 1
    if short_char_counter == 5:
        text = wordninja.split("".join(text).replace(" ", ""))
        return " ".join(text)
    else:
        return text


def remove_excess_punctuation(text) -> str:
    """
    param: text: Str document text
    returns: text: Str cleaned document text
        Returns text without excess punctuation
    """
    # Clean punctuation spacing
    text = text.replace(" .", "")
    for punc in string.punctuation:
        text = text.replace(punc + punc, "")
    return text


def preprocess(text) -> str:
    """
    param: text: Str document text
    returns: text: Str cleaned document text
        Function that fully preprocesses text
    """
    text = removing_regulator_names(text, regulator_name_list)
    text = delete_single_characters(text)
    text = remove_excess_punctuation(text)
    return text

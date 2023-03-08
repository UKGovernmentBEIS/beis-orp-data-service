import re
import nltk
from title_generation.preprocess.preprocess_functions import preprocess, removing_regulator_names
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
from title_generation.postprocess.postprocess_functions import postprocess_title
from title_generation.search_metadata_title.get_title import identify_metadata_title_in_text

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Bulk_processing")


# Define predictor function
def title_predictor(text: str) -> str:
    """
    param: text: Str document text
    returns: processed_title: Str cleaned predicted title from text from pretrained model
        Function to predict a title from the document text using a pretrained model
    """

    tokenizer = AutoTokenizer.from_pretrained(
        "fabiochiu/t5-small-medium-title-generation")
    model = AutoModelForSeq2SeqLM.from_pretrained(
        "fabiochiu/t5-small-medium-title-generation")

    # Preprocess the text
    text = preprocess(text)
    inputs = ["summarize: " + text]
    inputs = tokenizer(inputs, truncation=True, return_tensors="pt")
    output = model.generate(**inputs, num_beams=10,
                            do_sample=False, min_length=10, max_new_tokens=25)
    decoded_output = tokenizer.batch_decode(
        output, skip_special_tokens=True)[0]
    predicted_title = nltk.sent_tokenize(decoded_output.strip())[0]

    # Postprocess the text
    processed_title = postprocess_title(predicted_title)
    return processed_title


def get_title(title: str,
              text: str,
              threshold: str) -> str:
    """
    param: title: Str metadata title extracted from document
    param: text: Str document text
    param: threshold: int similarity score threshold
    returns: processed_title: Str cleaned predicted title from text from pretrained model
        Function that uses heuristics based on title length to either generate a title or
        use the metadata title
    """
    junk = ["Microsoft Word - ", ".Doc", ".doc"]

    # Remove junk
    for j in junk:
        title = re.sub(j, "", str(title))

    # Remove regulator names
    title = removing_regulator_names(title)

    # Remove excess whitespace
    title = re.sub(re.compile(r'\s+'), " ", title)

    # Immediately filter out long metadata titles
    if (len(title.split(" ")) > 40):
        title = title_predictor(text)
        return title

    else:
        score = identify_metadata_title_in_text(title, text)

        # If score is greater than 95% and title is less than / equal to 2 tokens
        length_of_no_punctuation_title = len(
            re.sub(r'[^\w\s]', ' ', title).split(" "))

        if score >= 95 and (length_of_no_punctuation_title <= 2):
            title = title_predictor(text)
            return title

        elif (score > threshold) and (length_of_no_punctuation_title >= 3):
            return title

        else:
            title = title_predictor(text)
            return title


def title_generator(text, metadata_title):

    title = get_title(title=metadata_title, text=text, threshold=85)
    logger.debug(f"Document title is: {title}")

    return title

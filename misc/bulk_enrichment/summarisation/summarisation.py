import os
import io
import torch
from summarisation.ext_sum import summarize
from summarisation.model_builder import ExtSummarizer
from smart_open import open as smart_open
import re

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Bulk_processing")

MODEL_BUCKET = 'summarisation/model_store/'

def download_model(
        bucket=MODEL_BUCKET,
        key='mobilebert_ext.pt'):
    '''Downloads the ML model for summarisation'''

    # Load the model in
    with smart_open(os.path.join(os.path.realpath(bucket), key), 'rb') as f:
        CHECKPOINT = io.BytesIO(f.read())
        checkpoint = torch.load(CHECKPOINT, map_location=torch.device("cpu"))
        model = ExtSummarizer(
            checkpoint=checkpoint,
            bert_type="mobilebert",
            device="cpu")

        return model


def smart_shortener(text):
    """
    params: text: Str
    returns: shortened_complete: Str (shortened text to summarise)
    """
    if len(text.split(" ")) < 600:
        return text
    else:
        shortened = " ".join(text.split(" ")[: 600])
        shortened_complete = shortened + text.replace(shortened, "").split(".")[0]
        return shortened_complete


def smart_postprocessor(sentence):
    if len(sentence.split(" ")) < 100:
        return sentence
    else:
        shortened = " ".join(sentence.split(" ")[ : 100])
        end_sentence = sentence.replace(shortened, "")
        shortened_complete = shortened + end_sentence.split(".")[0] + "."
        if len(shortened_complete) > 1000:
            res = [match.start() for match in re.finditer(r'[A-Z]', end_sentence)] 
            shortened_complete = shortened + end_sentence[:res[0] -1] + "."
            return shortened_complete
        else:
            return shortened_complete

def summariser(document):
    logger.info("Loading model")
    model = download_model()

    # Shorten text for summarising
    shortened_text = smart_shortener(text=document)
    summary = smart_postprocessor(summarize(
        shortened_text,
        model,
        max_length=4))

    logger.debug(f"Summary: {summary}")
    return summary

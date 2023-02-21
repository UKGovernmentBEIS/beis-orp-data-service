import os
import io
import torch
from ext_sum import summarize
from model_builder import ExtSummarizer
from smart_open import open as smart_open


import logging
logger = logging.getLogger("Bulk_processing").addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

MODEL_BUCKET = 'model_store/'
def download_model(
        bucket=MODEL_BUCKET,
        key='mobilebert_ext.pt'):
    '''Downloads the ML model for summarisation'''

    # Load the model in
    with smart_open(os.path.join(bucket, key), 'rb') as f:
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


def summariser(document):

    logger.info("Loading model")
    model = download_model()

    # Shorten text for summarising
    shortened_text = smart_shortener(text=document)
    summary = summarize(
        raw_text_fp=smart_shortener(
            text=shortened_text),
        model=model,
        max_length=4)

    return summary

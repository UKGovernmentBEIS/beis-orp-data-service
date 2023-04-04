import os
import io
import torch
from langdetect import detect
from smart_open import open
from summarisation.utils import smart_postprocessor, smart_shortener


from pyspark import SparkFiles
MODEL_PATH=  SparkFiles.get('resources')

def download_model( model_path,
        key='summ.pt'):
    '''Downloads the ML model for summarisation'''


    # Load the model in
    with open(os.path.join(os.path.realpath(model_path), key), 'rb') as f:
        model = io.BytesIO(f.read())
        summarizer = torch.load(model, map_location=torch.device('cpu'))
        return summarizer


def detect_language(text):
    """
    Detect language
    param: text: Str
        returns: Str: language of the document
    """
    language = detect(smart_shortener(text))
    return language


def summarizer(text):


    summarizer= download_model(MODEL_PATH)


    # Detect language
    lang = detect_language(text=text)

    # Shorten text after summarising
    summary = smart_postprocessor(
                    summarizer(text))

    return summary, lang
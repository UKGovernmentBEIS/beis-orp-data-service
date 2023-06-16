import os
from langdetect import detect
from transformers import pipeline
from utils import smart_postprocessor, smart_shortener
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

logger = Logger()


def validate_env_variable(env_var_name):
    logger.debug(
        f"Getting the value of the environment variable: {env_var_name}")
    try:
        env_variable = os.environ[env_var_name]
    except KeyError:
        raise Exception(f"Please, set environment variable {env_var_name}")
    if not env_variable:
        raise Exception(f"Please, provide environment variable {env_var_name}")
    return env_variable


def load_model(
        model="bart-large-cnn-samsum"):
    '''Downloads the ML model for summarisation'''
    summarizer = pipeline(
        "summarization",
        f"./LLM/{model}",
        max_length=600,
        truncation=True)
    return summarizer


def detect_language(text):
    """
    Detect language
    param: text: Str
        returns: Str: language of the document
    """
    language = detect(smart_shortener(text))
    return language


def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    text = event['text']

    summarizer = load_model()

    # Detect language
    lang = detect_language(text=text)

    # Shorten text after summarising
    summary = smart_postprocessor(
        summarizer(text)[0]["summary_text"])

    logger.info(f'Langauge: {lang}')
    logger.info(f'Summary: {summary}')

    return {'summary': summary, 'lang': lang}

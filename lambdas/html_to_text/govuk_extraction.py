import re
import requests
from bs4 import BeautifulSoup


def find_key(key, dictionary):
    """
    Recursive function to find all text in body elements of the api/content
    params: key: Str (body)
    dictionary: json of api/content
        returns: generator object of all text in body element
    """
    if key in dictionary:
        yield dictionary[key]
    for value in dictionary.values():
        if isinstance(value, dict):
            yield from find_key(key, value)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    yield from find_key(key, item)


def get_content(url):
    """
    Extract title, text, date from gov.uk urls
    params: url: Str
        returns text, title, date_published
    """

    url_json = url.replace("gov.uk/", "gov.uk/api/content/")
    response = requests.get(url)

    soup = BeautifulSoup(response.text, 'html.parser')

    # Title
    title = re.sub('\s+', " ", soup.find("title").get_text()).strip().replace("\n", " ").replace(" - GOV.UK","")

    js = requests.get(url_json).json()

    # Text
    content = []
    for v in find_key("body", js):
        text = " ".join([p.get_text()
                        for p in BeautifulSoup(v, features="html.parser")])
        content.append(text)
    text = re.sub('\\s+', " ", " ".join(content)).strip().replace("\n", " ")

    # Date
    date_published = js["public_updated_at"]

    return text, title, date_published

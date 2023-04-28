import re


def smart_shortener(text):
    '''
    params: text: Str
    returns: shortened_complete: Str (shortened text to summarise)
    '''
    if len(text.split(' ')) < 600:
        return text
    else:
        shortened = ' '.join(text.split(' ')[: 600])
        shortened_complete = shortened + \
            text.replace(shortened, '').split('.')[0]
        return shortened_complete


def smart_postprocessor(sentence):
    if len(sentence) < 500:
        return sentence
    else:
        shortened = sentence[: 500]
        end_sentence = sentence.replace(shortened, '')
        shortened_complete = shortened + end_sentence.split('.')[0] + '.'
        if len(shortened_complete) > 600:
            res = [match.start()
                   for match in re.finditer(r'[A-Z]', end_sentence)]
            shortened_complete = shortened + end_sentence[:res[0] - 1] + '.'
            return shortened_complete
        else:
            return shortened_complete

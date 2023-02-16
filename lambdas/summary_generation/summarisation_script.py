import torch
from model_builder import ExtSummarizer
from ext_sum import summarize

def load_model(model_type):
    # Load model checkpoint
    checkpoint = torch.load(f"checkpoints/{model_type}_ext.pt", map_location="cpu")
    model = ExtSummarizer(checkpoint=checkpoint, bert_type=model_type, device="cpu")
    return model

def smart_shortener(text):
    if len(text.split(" ")) < 600:
        return text
    else:
        shortened = " ".join(text.split(" ")[ : 600])
        shortened_complete = shortened + text.replace(shortened, "").split(".")[0]
        return shortened_complete

def clean_summary(summary):
    summary_list = summary.strip().split(" ")
    enum_summary = enumerate(summary_list)
    for idx, word in enum_summary:
        if word.isupper() == False and enum_summary[idx + 1].isupper() == True:
            summary_list.insert(idx + 1, ".")
    return " ".join(summary_list)

def lambda():
    model = load_model("mobilebert")
    shortend_text = smart_shortener(text):
    summary = summarize(smart_shortener(shortend_text), model, max_length=4)
    cleaned_summary = clean_summary(summary)
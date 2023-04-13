from transformers import AutoTokenizer, AutoModelForSeq2SeqLM

tokenizer = AutoTokenizer.from_pretrained(
        'fabiochiu/t5-small-medium-title-generation')
model = AutoModelForSeq2SeqLM.from_pretrained(
        'fabiochiu/t5-small-medium-title-generation')

model.save_pretrained('./LLM/t5_model')
tokenizer.save_pretrained('./LLM/t5_tokenizer')
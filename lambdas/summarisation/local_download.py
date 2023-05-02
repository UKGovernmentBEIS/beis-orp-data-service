from transformers import pipeline

summarizer = pipeline("summarization", model="philschmid/bart-large-cnn-samsum")

summarizer.save_pretrained("./LLM/bart-large-cnn-samsum")

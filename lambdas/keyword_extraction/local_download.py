import torch
from keybert import KeyBERT

kw_model = KeyBERT(model='all-MiniLM-L6-v2')
torch.save(kw_model, "./LLM/keybert.pt")
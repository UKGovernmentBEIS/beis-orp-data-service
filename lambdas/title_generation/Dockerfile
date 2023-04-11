FROM public.ecr.aws/lambda/python:3.9

RUN yum -y update
RUN yum -y install gcc-c++
# RUN apt-get update && apt-get install unzip

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Set environment variables
ENV NUMBA_CACHE_DIR=/tmp/
ENV TRANSFORMERS_CACHE=/tmp/
ENV NLTK_DATA={LAMBDA_TASK_ROOT}/nltk_data/
ENV SPACY_PATH=/tmp/spacy_model/
ENV MODEL_PATH=/tmp/modeldir/

# Download Spacy 
RUN python -m spacy download en_core_web_lg

# Create directory and download NLTK
RUN mkdir -p ${LAMBDA_TASK_ROOT}/nltk_data/
RUN python -m nltk.downloader -d ${LAMBDA_TASK_ROOT}/nltk_data/ all
# RUN python -m nltk.downloader -d ${LAMBDA_TASK_ROOT}/nltk_data/ wordnet
# RUN python -m nltk.downloader -d ${LAMBDA_TASK_ROOT}/nltk_data/ omw-1.4
# RUN python -m nltk.downloader -d ${LAMBDA_TASK_ROOT}/nltk_data/ stopwords

# RUN [ "python3", "-c", "import os; os.makedirs('/tmp/nltk_data', exist_ok=True)" ]
# RUN [ "python3", "-c", "import nltk; nltk.download('punkt', download_dir='./nltk_data/')" ]
# RUN [ "python3", "-c", "import nltk; nltk.download('wordnet', download_dir='./nltk_data/')" ]
# RUN [ "python3", "-c", "import nltk; nltk.download('omw-1.4', download_dir='./nltk_data/')" ]
# RUN [ "python3", "-c", "import nltk; nltk.download('stopwords', download_dir='./nltk_data/')" ]

# Unzip
# RUN unzip /tmp/nltk_data/punkt.zip && rm -f /tmp/nltk_data/punkt.zip

# Add utilities for the script
ADD postprocess ${LAMBDA_TASK_ROOT}/postprocess 
ADD preprocess ${LAMBDA_TASK_ROOT}/preprocess
ADD search_metadata_title ${LAMBDA_TASK_ROOT}/search_metadata_title 
# ADD nltk_data ${LAMBDA_TASK_ROOT}/nltk_data

# Copy function code
COPY title_generation.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "title_generation.handler" ]

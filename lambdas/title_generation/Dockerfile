FROM public.ecr.aws/lambda/python:3.10

RUN yum -y update
RUN yum -y install gcc-c++

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Set environment variables
ENV NUMBA_CACHE_DIR=/tmp/
ENV TRANSFORMERS_CACHE=/tmp/
ENV NLTK_DATA=/tmp/nltk_data/
ENV SPACY_PATH=/tmp/spacy_model/

# Download Spacy 
RUN python -m spacy download en_core_web_lg

# Make model directory and download models locally
RUN mkdir -p ${LAMBDA_TASK_ROOT}/LLM
COPY local_download.py ${LAMBDA_TASK_ROOT}
RUN python local_download.py

# Add utilities for the script
ADD postprocess ${LAMBDA_TASK_ROOT}/postprocess 
ADD preprocess ${LAMBDA_TASK_ROOT}/preprocess
ADD search_metadata_title ${LAMBDA_TASK_ROOT}/search_metadata_title 

# Copy function code
COPY title_generation.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "title_generation.handler" ]

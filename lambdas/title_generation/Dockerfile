FROM public.ecr.aws/lambda/python:3.9

RUN yum -y update
RUN yum -y install gcc-c++

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"
RUN python -m spacy download en_core_web_lg

# Set environment variables
ENV NUMBA_CACHE_DIR=/tmp/
ENV TRANSFORMERS_CACHE=/tmp/
ENV NLTK_DATA=/tmp/nltk_data/
ENV SPACY_PATH=/tmp/spacy_model/
ENV SOURCE_BUCKET=beis-orp-dev-datalake
ENV MODEL_PATH=/tmp/modeldir/
ENV MODEL_BUCKET=beis-orp-dev-clustering-models
ENV DOCUMENT_DATABASE=mongodb://ddbadmin:Test123456789@beis-orp-dev-beis-orp.cluster-cau6o2mf7iuc.eu-west-2.docdb.amazonaws.com:27017/?directConnection=true

# Add preprocess and postprocess folders
ADD postprocess ${LAMBDA_TASK_ROOT}/postprocess 
ADD preprocess ${LAMBDA_TASK_ROOT}/preprocess 
ADD search_metadata_title ${LAMBDA_TASK_ROOT}/search_metadata_title 

# Copy function code
COPY title_extraction.py ${LAMBDA_TASK_ROOT}
COPY rds-combined-ca-bundle.pem ${LAMBDA_TASK_ROOT}
COPY nltk_data/ ${LAMBDA_TASK_ROOT}/nltk_data 
COPY preprocess/regulator_name_list.txt ${LAMBDA_TASK_ROOT}/preprocess/regulator_name_list.txt

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "title_generation.handler" ]
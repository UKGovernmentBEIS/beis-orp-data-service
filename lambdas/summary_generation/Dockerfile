FROM public.ecr.aws/lambda/python:3.9

RUN yum -y update
RUN yum -y install gcc-c++

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Set environment variables
# ENV NUMBA_CACHE_DIR=/tmp/
# ENV TRANSFORMERS_CACHE=/tmp/
# ENV NLTK_DATA=/tmp/nltk_data/
ENV SOURCE_BUCKET=beis-orp-dev-datalake
ENV DOCUMENT_DATABASE=mongodb://ddbadmin:Test123456789@beis-orp-dev-beis-orp.cluster-cau6o2mf7iuc.eu-west-2.docdb.amazonaws.com:27017/?directConnection=true

# Copy function code
COPY summarisation_script.py ${LAMBDA_TASK_ROOT}
COPY model_builder.py ${LAMBDA_TASK_ROOT}
COPY ext_sum.py ${LAMBDA_TASK_ROOT}
COPY MobileBert ${LAMBDA_TASK_ROOT}
COPY rds-combined-ca-bundle.pem ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "summarisation_script.handler" ]

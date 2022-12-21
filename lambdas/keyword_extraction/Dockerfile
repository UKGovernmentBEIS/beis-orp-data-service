FROM public.ecr.aws/lambda/python:3.9

RUN yum -y update
RUN yum -y install gcc-c++

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy function code
COPY lambda_function.py ${LAMBDA_TASK_ROOT}
# COPY preprocess/ ${LAMBDA_TASK_ROOT}/preprocess/
COPY stopwords.txt ${LAMBDA_TASK_ROOT}
COPY rds-combined-ca-bundle.pem ${LAMBDA_TASK_ROOT}
COPY nltk_data/ ./nltk_data ${LAMBDA_TASK_ROOT}

# Set tmp directory
ENV NUMBA_CACHE_DIR=/tmp/
ENV TRANSFORMERS_CACHE=/tmp/
ENV NLTK_DATA=${LAMBDA_TASK_ROOT}/nltk_data

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda_function.handler" ]
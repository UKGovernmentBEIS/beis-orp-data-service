FROM public.ecr.aws/lambda/python:3.9

RUN yum -y update
RUN yum -y install gcc-c++

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Set environment variables
ENV NUMBA_CACHE_DIR=/tmp/
ENV TRANSFORMERS_CACHE=/tmp/
ENV NLTK_DATA=/tmp/nltk_data/

# Add MobileBert folder
ADD MobileBert/ ${LAMBDA_TASK_ROOT}/MobileBert
ADD checkpoints ${LAMBDA_TASK_ROOT}/checkpoints 

# Copy function code and supporting packages
COPY nltk_data/ ${LAMBDA_TASK_ROOT}/nltk_data/
COPY summarisation.py ${LAMBDA_TASK_ROOT}
COPY model_builder.py ${LAMBDA_TASK_ROOT}
COPY ext_sum.py ${LAMBDA_TASK_ROOT}
COPY encoder.py ${LAMBDA_TASK_ROOT}
COPY neural.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "summarisation.handler" ]

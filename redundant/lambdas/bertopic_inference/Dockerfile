FROM public.ecr.aws/lambda/python:3.9

RUN yum -y update
RUN yum -y install gcc-c++

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy function code
COPY bertopic_inference.py ${LAMBDA_TASK_ROOT}
COPY preprocess/ ${LAMBDA_TASK_ROOT}/preprocess/
COPY rds-combined-ca-bundle.pem ${LAMBDA_TASK_ROOT}

# Set tmp directory
ENV NUMBA_CACHE_DIR=/tmp/

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "bertopic_inference.handler" ]

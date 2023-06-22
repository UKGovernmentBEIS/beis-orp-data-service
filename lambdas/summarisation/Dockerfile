FROM public.ecr.aws/lambda/python:3.9

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Set env vars
ENV NUMBA_CACHE_DIR=/tmp/
ENV TRANSFORMERS_CACHE=/tmp/

# Make model directory and download models locally
RUN mkdir -p ${LAMBDA_TASK_ROOT}/LLM
COPY local_download.py ${LAMBDA_TASK_ROOT}
RUN python local_download.py

# Copy scripts to lambda root
COPY utils.py ${LAMBDA_TASK_ROOT}
COPY summarisation.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "summarisation.handler" ]

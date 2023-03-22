FROM public.ecr.aws/lambda/python:3.9

COPY requirements.txt  .
RUN pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"
RUN python -m spacy download en_core_web_sm

# Copy function code
COPY legislative_origin_extraction.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "legislative_origin_extraction.handler" ]

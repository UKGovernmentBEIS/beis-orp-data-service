FROM public.ecr.aws/lambda/python:3.9

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"
RUN python -m spacy download en_core_web_sm

# Set environment variables
ENV SOURCE_BUCKET=beis-orp-dev-datalake
ENV DOCUMENT_DATABASE=mongodb://ddbadmin:Test123456789@beis-orp-dev-beis-orp.cluster-cau6o2mf7iuc.eu-west-2.docdb.amazonaws.com:27017/?directConnection=true

# Copy function code
COPY date_from_text.py ${LAMBDA_TASK_ROOT}
COPY rds-combined-ca-bundle.pem ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "date_from_text.handler" ]

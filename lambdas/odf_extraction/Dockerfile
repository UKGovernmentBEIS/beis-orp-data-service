FROM public.ecr.aws/lambda/python:3.9

# Install the function's dependencies using file requirements.txt
# from your project folder.
COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Set environment variables
ENV DOCUMENT_DATABASE=mongodb://ddbadmin:Test123456789@beis-orp-dev-beis-orp.cluster-cau6o2mf7iuc.eu-west-2.docdb.amazonaws.com:27017/?directConnection=true
ENV DESTINATION_BUCKET=beis-orp-dev-datalake
ENV SOURCE_BUCKET=beis-orp-dev-datalake

# Copy function code and certificates
COPY odf_extract_all.py ${LAMBDA_TASK_ROOT}
COPY rds-combined-ca-bundle.pem ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "odf_extract_all.handler" ]
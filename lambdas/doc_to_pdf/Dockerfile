FROM public.ecr.aws/lambda/python:3.9

# Install the function's dependencies using file requirements.txt
COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Set environment variables
ENV DESTINATION_BUCKET=beis-orp-dev-upload

# Copy function code and certificates
COPY doc_to_pdf.py ${LAMBDA_TASK_ROOT}
COPY rds-combined-ca-bundle.pem ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "doc_to_pdf.handler" ]

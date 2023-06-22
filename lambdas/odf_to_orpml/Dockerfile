FROM public.ecr.aws/lambda/python:3.9

# Install the function's dependencies using requirements.txt
COPY requirements.txt  .
RUN pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy code and certificates
COPY odf_to_orpml.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "odf_to_orpml.handler" ]

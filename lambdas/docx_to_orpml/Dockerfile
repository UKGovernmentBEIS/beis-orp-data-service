FROM public.ecr.aws/lambda/python:3.9

# Install the function's dependencies using requirements.txt
COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy function code
COPY docx_to_orpml.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "docx_to_orpml.handler" ]

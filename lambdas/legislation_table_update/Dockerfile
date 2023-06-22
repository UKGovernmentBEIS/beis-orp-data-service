FROM public.ecr.aws/lambda/python:3.9

COPY requirements.txt  .
RUN pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy code and leg-division-list.csv
COPY legislation_table_update.py ${LAMBDA_TASK_ROOT}
COPY leg-division-list.csv ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "legislation_table_update.handler" ]

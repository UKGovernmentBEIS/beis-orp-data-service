FROM public.ecr.aws/lambda/python:3.9

# Install the function's dependencies using file requirements.txt
# from your project folder.
COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy function code and certificates
ADD word_forms_loc.tar.gz ${LAMBDA_TASK_ROOT}
RUN chown root:root -R ${LAMBDA_TASK_ROOT}/word_forms_loc
RUN chmod 755 -R ${LAMBDA_TASK_ROOT}/word_forms_loc
COPY handler.py ${LAMBDA_TASK_ROOT}
COPY nltk_data ./nltk_data 

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "handler.lambda_handler" ]

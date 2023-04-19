FROM public.ecr.aws/lambda/python:3.9

# Install the function's dependencies using requirements.txt
COPY requirements.txt  .
RUN pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Environment Variables
ENV NLTK_DATA={LAMBDA_TASK_ROOT}/nltk_data/

# Create directory and download NLTK
RUN mkdir -p ${LAMBDA_TASK_ROOT}/nltk_data/
RUN python -m nltk.downloader -d ${LAMBDA_TASK_ROOT}/nltk_data/ all

# Copy function code and certificates
COPY stopwords.txt ${LAMBDA_TASK_ROOT}
COPY utils.py ${LAMBDA_TASK_ROOT}
COPY notification_email.py ${LAMBDA_TASK_ROOT}
COPY check_duplicate.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "check_duplicate.handler" ]
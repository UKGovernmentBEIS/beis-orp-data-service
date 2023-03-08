FROM public.ecr.aws/lambda/python:3.9

RUN yum -y update
RUN yum -y install gcc-c++

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Set environment variables
ENV NUMBA_CACHE_DIR=/tmp/
ENV TRANSFORMERS_CACHE=/tmp/
ENV NLTK_DATA=/tmp/nltk_data/
ENV MODEL_PATH=/tmp/modeldir/

# Copy function code
ADD word_forms_loc.tar.gz ${LAMBDA_TASK_ROOT}
RUN chown root:root -R ${LAMBDA_TASK_ROOT}/word_forms_loc
RUN chmod 755 -R ${LAMBDA_TASK_ROOT}/word_forms_loc
COPY keyword_extraction.py ${LAMBDA_TASK_ROOT}
COPY stopwords.txt ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "keyword_extraction.handler" ]

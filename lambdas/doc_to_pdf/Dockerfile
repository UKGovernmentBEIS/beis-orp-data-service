FROM public.ecr.aws/lambda/python:3.9

# RUN uname -m
# RUN yum -y update

# ADD --chown=root:root LibreOffice_7.4.5_MacOS_aarch64.dmg /tmp/libreoffice
# RUN chmod 755 -R /tmp/*
# RUN hdiutil attach /tmp/libreoffice
# RUN yum install -y /tmp/libreoffice/LibreOffice_7.4.5.1_Linux_x86-64_rpm/RPMS/*.rpm
# RUN soffice

# Install the function's dependencies using file requirements.txt
COPY lo.tar.br /opt/
RUN chmod 755 -R /tmp/
COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Set environment variables
ENV DESTINATION_BUCKET=beis-orp-dev-upload

# Copy function code and certificates
COPY doc_to_pdf.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "doc_to_pdf.handler" ]

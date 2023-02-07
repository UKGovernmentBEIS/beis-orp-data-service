FROM public.ecr.aws/lambda/python:3.9

# Install LibreOffice and dependencies
RUN yum -y update
ADD --chown=root:root lo-x86-64-rpm.tar.gz /tmp/libreoffice
RUN chmod 755 -R /tmp/*
RUN yum install -y /tmp/libreoffice/LibreOffice_7.4.5.1_Linux_x86-64_rpm/RPMS/*.rpm
RUN yum install -y libXinerama.x86_64 cups-libs dbus-glib cairo java

# Install the function's dependencies using file requirements.txt
COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Set environment variables
ENV DESTINATION_BUCKET=beis-orp-dev-upload
ENV SOFFICE_PATH=/opt/libreoffice7.4/program/soffice

# Copy function code and certificates
COPY doc_to_pdf.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "doc_to_pdf.handler" ]

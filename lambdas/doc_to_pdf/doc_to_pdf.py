import brotli
import tarfile
from io import BytesIO
import os
import subprocess
import boto3
# from http import HTTPStatus
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']

libre_office_install_dir = '/tmp/instdir'


def load_libre_office():
    os.makedirs(libre_office_install_dir, exist_ok=True)

    buffer = BytesIO()
    with open('/opt/lo.tar.br', 'rb') as brotli_file:
        d = brotli.Decompressor()
        while True:
            chunk = brotli_file.read(1024)
            buffer.write(d.process(chunk))
            if len(chunk) < 1024:
                break
        buffer.seek(0)

    with tarfile.open(fileobj=buffer) as tar:
        tar.extractall('/tmp')
    return f'{libre_office_install_dir}/program/soffice.bin'


def download_doc(s3_client, object_key, source_bucket, file_path):
    '''Downloads the PDF from S3 ready for conversion and metadata extraction'''

    s3_client.download_file(
        Bucket=source_bucket,
        Key=object_key,
        Filename=file_path
    )

    return os.listdir('/tmp/')


def get_s3_metadata(s3_client, object_key, source_bucket):
    '''Gets the S3 metadata attached to the PDF'''

    metadata = s3_client.head_object(
        Bucket=source_bucket,
        Key=object_key
    )['Metadata']

    return metadata


def convert_word_to_pdf(soffice_path, word_file_path, output_dir):
    # conv_cmd = (f"{soffice_path} --headless --norestore --invisible --nodefault --nofirststartwizard --nolockcheck"
    #             f" --nologo --convert-to pdf:writer_pdf_Export --outdir {output_dir} {word_file_path}")
    # response = subprocess.run(
    #     conv_cmd.split(),
    #     stdout=subprocess.PIPE,
    #     stderr=subprocess.PIPE)
    # logger.info(response)
    # if response.returncode != 0:
    #     response = subprocess.run(
    #         conv_cmd.split(),
    #         stdout=subprocess.PIPE,
    #         stderr=subprocess.PIPE)
    #     if response.returncode != 0:
    #         return False
    response = subprocess.call([soffice_path, '--headless', '--convert-to',
                                'pdf', '--outdir', output_dir, word_file_path])
    logger.info(response)
    return response


def upload_pdf(s3_client, object_key, metadata,
               destination_bucket=DESTINATION_BUCKET):
    '''Write the extracted text to a .txt file in the staging bucket'''

    response = s3_client.upload_file(
        Filename=f'/tmp/{object_key}',
        Bucket=destination_bucket,
        Key=f'docs/{object_key}.pdf',
        # Metadata=metadata
    )
    logger.info('Saved converted document back to S3')

    return response


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    source_bucket = event['detail']['bucket']['name']
    object_key = event['detail']['object']['key']
    logger.info(
        f'New document in {source_bucket}: {object_key}')

    soffice_path = load_libre_office()
    logger.info(soffice_path)

    s3_client = boto3.client('s3')

    response = download_doc(
        s3_client=s3_client,
        object_key=object_key,
        source_bucket=source_bucket,
        file_path=f'/tmp/{object_key}')
    logger.info(response)

    doc_s3_metadata = get_s3_metadata(
        s3_client=s3_client,
        object_key=object_key,
        source_bucket=source_bucket)
    logger.info(doc_s3_metadata)
    # document_uid = doc_s3_metadata['uuid']
    # logger.append_keys(document_uid=document_uid)

    pdf_document = convert_word_to_pdf(
        soffice_path=soffice_path,
        word_file_path=f'/tmp/{object_key}',
        output_dir='/tmp')
    logger.info(pdf_document)
    logger.info(os.listdir('/tmp/'))

    upload_pdf(
        s3_client=s3_client,
        object_key=object_key,
        metadata=doc_s3_metadata)

    # handler_response = {**s3_response}
    # handler_response['document_uid'] = document_uid

    return None

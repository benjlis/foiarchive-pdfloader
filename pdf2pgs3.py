import aiosql
import psycopg2
import os
import requests
import time
import pdftotext
import boto3

PDFDIR = os.getenv('PDFDIR')
SLEEP_DURATION = 45             # seconds

# db-related configuration
conn = psycopg2.connect("")
conn.autocommit = True
stmts = aiosql.from_path("pdf2pg.sql", "psycopg2")


def download_pdf(url, dfile):
    response = requests.get(url)
    with open(dfile, 'wb') as f:
        f.write(response.content)
    return response.status_code


def upload_s3(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_name, bucket, object_name)
    except Exception as e:
        print(e)
        return False
    return True


pdfs = stmts.get_pdf_list(conn)
for p in pdfs:
    cnt = p[0]
    id = p[1]
    pdf_url = p[2]
    http_status = None
    pdf_file = pdf_url.rsplit('/')[-1]
    # pdf_file = pdf_file[:-3] + 'pdf'
    pdf_file_path = PDFDIR + pdf_file
    while http_status != 200:
        http_status = download_pdf(pdf_url, pdf_file_path)
        pdf_size = os.stat(pdf_file_path).st_size
        with open(pdf_file_path, "rb") as f:
            pdf = pdftotext.PDF(f, physical=True)
        pg_cnt = len(pdf)
        stmts.add_pdf(conn, oai_id=id, pg_cnt=pg_cnt, size=pdf_size)
        pg = 1
        for page in pdf:
            word_cnt = len(page.split())
            char_cnt = len(page)
            stmts.add_pdfpage(conn, oai_id=id, pg=pg, word_cnt=word_cnt,
                              char_cnt=char_cnt, body=page)
            pg += 1
        s3_status = upload_s3(pdf_file_path, 'foiarchive-un',
                              'moon/' + pdf_file)
        print(f'{cnt=}, {id=}, {pdf_file=}, {http_status=}, {pdf_size=}, \
{pg_cnt=}, {s3_status=}')
        os.remove(pdf_file_path)
        time.sleep(SLEEP_DURATION)

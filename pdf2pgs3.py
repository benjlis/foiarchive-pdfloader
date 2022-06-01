import argparse
import aiosql
import psycopg2
import os
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pdftotext
import boto3
import datetime

# get environmental variables
PDFDIR = os.getenv('PDFDIR')

# process command line arguments
parser = argparse.ArgumentParser(description='Downloads PDFs from a site and \
loads text into DB and pushes a copy to s3')
parser.add_argument('first_id', type=int, help='first oai_id in range')
parser.add_argument('last_id', type=int, help='last oai_id in range')
args = parser.parse_args()
print(f'pdf2pgs3: processing ids between {args.first_id} and {args.last_id}')

# db-related configuration
conn = psycopg2.connect("")
conn.autocommit = True
stmts = aiosql.from_path("pdf2pg.sql", "psycopg2")

# establish requests session
headers = {'User-Agent': 'unarms-requests'}
http = requests.Session()
http.mount("https://", HTTPAdapter(max_retries=Retry(backoff_factor=90)))


def download_pdf(url, dfile):
    response = http.get(url, headers=headers)
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


pdfs = stmts.get_pdf_list(conn, first_id=args.first_id, last_id=args.last_id)
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
    s3_status = upload_s3(pdf_file_path, 'foiarchive-un', 'annan/' + pdf_file)
    now = datetime.datetime.now().strftime('%m-%d %H:%M:%S')
    print(f'{now}, {cnt=}, {id=}, {pdf_file=}, {http_status=}, {pdf_size=}, \
{pg_cnt=}, {s3_status=}')
    os.remove(pdf_file_path)

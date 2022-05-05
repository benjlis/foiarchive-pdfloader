import aiosql
import psycopg2
import os
import requests
import time
import pdftotext

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


pdfs = stmts.get_pdf_list(conn)
for p in pdfs:
    cnt = p[0]
    id = p[1]
    pdf_url = p[2]
    http_status = None
    pdf_file = pdf_url.rsplit('/')[-1]
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
            # print(f'{cnt=}, {id=}, {pg=}, {word_cnt=}, {char_cnt=}')
            # print(page)
            stmts.add_pdfpage(conn, oai_id=id, pg=pg, word_cnt=word_cnt,
                              char_cnt=char_cnt, body=page)
            pg += 1
        print(f'{cnt=}, {id=}, {pdf_file=}, {http_status=}, {pdf_size=}, \
{pg_cnt=}')
        time.sleep(SLEEP_DURATION)

# stmts.insert_email(conn, file_id=32, file_pg_start=1, pg_cnt=4,
#                   header_begin_ln=2, header_end_ln=8, from_email='xx@abc.com',
#                   to_emails='yyy@def.com', cc_emails='zzz@def.com',
#                   attachments='a.pdf', importance='', subject='test email',
#                   body='line1\nline2\nline3')

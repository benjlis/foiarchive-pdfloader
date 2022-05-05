-- name: get-pdf-list
-- Get list of all pdfs that can be processed
select row_number() over () , oai_id, pdf_url
    from un_archives.metadata_load
    where pdf_url is not null
    order by oai_id
    limit 5;

-- name: add-pdf!
-- Add row to pdfpages
insert into un_archives.pdfs(oai_id, pg_cnt, size)
values (:oai_id, :pg_cnt, :size);

-- name: add-pdfpage!
-- Add row to pdfpages
insert into un_archives.pdfpages(oai_id, pg, word_cnt, char_cnt, body)
values (:oai_id, :pg, :word_cnt, :char_cnt, :body);

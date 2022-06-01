-- name: get-pdf-list
-- Get list of all pdfs that can be processed
select row_number() over () , m.oai_id, pdf_url
    from un_archives.metadata m left join un_archives.pdfs p
                                        on (m.oai_id = p.oai_id)
    where pdf_url is not null and
          m.oai_id between :first_id and :last_id and
          p.oai_id is null
    order by oai_id;

-- name: add-pdf!
-- Add row to pdfpages
insert into un_archives.pdfs(oai_id, pg_cnt, size)
values (:oai_id, :pg_cnt, :size);

-- name: add-pdfpage!
-- Add row to pdfpages
insert into un_archives.pdfpages(oai_id, pg, word_cnt, char_cnt, body)
values (:oai_id, :pg, :word_cnt, :char_cnt, :body);

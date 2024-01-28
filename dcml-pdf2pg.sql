-- name: get-pdf-list
-- Get list of all pdfs that can be processed
select row_number() over (order by item_id), i.item_id,
    concat('https://s3.documentcloud.org/documents/', i.item_id, '/',
           i.slug, '.pdf') pdf_url
    from covid19.dcml_items i
    where item_id between :first_id and :last_id and
          subset = 'train' and
          not exists (select 1 from covid19.dcml_pdfs
                         where item_id = i.item_id)
    order by item_id;
-- name: add-pdf!
-- Add row to pdfpages
insert into covid19.dcml_pdfs(item_id, filename, pg_cnt, size)
values (:id, :pdf_file, :pg_cnt, :size);

-- name: add-pdfpage!
-- Add row to pdfpages
insert into  covid19.dcml_pdfpages(item_id, pg, word_cnt, char_cnt, body)
values (:id, :pg, :word_cnt, :char_cnt, :body);

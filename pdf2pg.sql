-- name: get-pdf-list
-- Get list of all pdfs that can be processed
select row_number() over () , el.hlid, el.digitalobjecturi
    from nato_archives.export_load el
    where digitalobjecturi is not null and
          identifier is not null and
          hlid between :first_id and :last_id and
          (culture = 'en' or
           (culture = 'fr' and
            not exists (select 1 from nato_archives.export_load
                           where culture = 'en' and
                                 identifier = el.identifier)))
    order by hlid;
-- name: add-pdf!
-- Add row to pdfpages
insert into nato_archives.pdfs(item_id, pg_cnt, size)
values (:id, :pg_cnt, :size);

-- name: add-pdfpage!
-- Add row to pdfpages
insert into nato_archives.pdfpages(item_id, pg, word_cnt, char_cnt, body)
values (:id, :pg, :word_cnt, :char_cnt, :body);

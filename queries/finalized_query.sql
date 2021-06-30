/* Grab the needed WARC data for distinct Yahoo News Japan articles crawled
between February 2020 and May 2021. Sample 50% of the matches, limited to the first 65,000 articles. */
WITH all_partitions AS
    (SELECT DISTINCT(url),
         warc_filename,
         warc_record_offset,
         warc_record_length
    FROM "ccindex"."ccindex"
    WHERE crawl = 'CC-MAIN-2021-21'
            AND subset = 'warc'
            AND url_host_name = 'news.yahoo.co.jp'
            AND url LIKE '%/articles/%'
            AND url NOT LIKE '%/images/%'
            AND fetch_status = 200
    UNION
    SELECT DISTINCT(url),
         warc_filename,
         warc_record_offset,
         warc_record_length
    FROM "ccindex"."ccindex"
    WHERE crawl = 'CC-MAIN-2021-17'
            AND subset = 'warc'
            AND url_host_name = 'news.yahoo.co.jp'
            AND url LIKE '%/articles/%'
            AND url NOT LIKE '%/images/%'
            AND fetch_status = 200
    UNION
    SELECT DISTINCT(url),
         warc_filename,
         warc_record_offset,
         warc_record_length
    FROM "ccindex"."ccindex"
    WHERE crawl = 'CC-MAIN-2021-10'
            AND subset = 'warc'
            AND url_host_name = 'news.yahoo.co.jp'
            AND url LIKE '%/articles/%'
            AND url NOT LIKE '%/images/%'
            AND fetch_status = 200
    UNION
    SELECT DISTINCT(url),
         warc_filename,
         warc_record_offset,
         warc_record_length
    FROM "ccindex"."ccindex"
    WHERE crawl = 'CC-MAIN-2021-04'
            AND subset = 'warc'
            AND url_host_name = 'news.yahoo.co.jp'
            AND url LIKE '%/articles/%'
            AND url NOT LIKE '%/images/%'
            AND fetch_status = 200
    UNION
    SELECT DISTINCT(url),
         warc_filename,
         warc_record_offset,
         warc_record_length
    FROM "ccindex"."ccindex"
    WHERE crawl = 'CC-MAIN-2020-50'
            AND subset = 'warc'
            AND url_host_name = 'news.yahoo.co.jp'
            AND url LIKE '%/articles/%'
            AND url NOT LIKE '%/images/%'
            AND fetch_status = 200
    UNION
    SELECT DISTINCT(url),
         warc_filename,
         warc_record_offset,
         warc_record_length
    FROM "ccindex"."ccindex"
    WHERE crawl = 'CC-MAIN-2020-45'
            AND subset = 'warc'
            AND url_host_name = 'news.yahoo.co.jp'
            AND url LIKE '%/articles/%'
            AND url NOT LIKE '%/images/%'
            AND fetch_status = 200
    UNION
    SELECT DISTINCT(url),
         warc_filename,
         warc_record_offset,
         warc_record_length
    FROM "ccindex"."ccindex"
    WHERE crawl = 'CC-MAIN-2020-40'
            AND subset = 'warc'
            AND url_host_name = 'news.yahoo.co.jp'
            AND url LIKE '%/articles/%'
            AND url NOT LIKE '%/images/%'
            AND fetch_status = 200
    UNION
    SELECT DISTINCT(url),
         warc_filename,
         warc_record_offset,
         warc_record_length
    FROM "ccindex"."ccindex"
    WHERE crawl = 'CC-MAIN-2020-34'
            AND subset = 'warc'
            AND url_host_name = 'news.yahoo.co.jp'
            AND url LIKE '%/articles/%'
            AND url NOT LIKE '%/images/%'
            AND fetch_status = 200
    UNION
    SELECT DISTINCT(url),
         warc_filename,
         warc_record_offset,
         warc_record_length
    FROM "ccindex"."ccindex"
    WHERE crawl = 'CC-MAIN-2020-29'
            AND subset = 'warc'
            AND url_host_name = 'news.yahoo.co.jp'
            AND url LIKE '%/articles/%'
            AND url NOT LIKE '%/images/%'
            AND fetch_status = 200
    UNION
    SELECT DISTINCT(url),
         warc_filename,
         warc_record_offset,
         warc_record_length
    FROM "ccindex"."ccindex"
    WHERE crawl = 'CC-MAIN-2020-24'
            AND subset = 'warc'
            AND url_host_name = 'news.yahoo.co.jp'
            AND url LIKE '%/articles/%'
            AND url NOT LIKE '%/images/%'
            AND fetch_status = 200
    UNION
    SELECT DISTINCT(url),
         warc_filename,
         warc_record_offset,
         warc_record_length
    FROM "ccindex"."ccindex"
    WHERE crawl = 'CC-MAIN-2020-16'
            AND subset = 'warc'
            AND url_host_name = 'news.yahoo.co.jp'
            AND url LIKE '%/articles/%'
            AND url NOT LIKE '%/images/%'
            AND fetch_status = 200
    UNION
    SELECT DISTINCT(url),
         warc_filename,
         warc_record_offset,
         warc_record_length
    FROM "ccindex"."ccindex"
    WHERE crawl = 'CC-MAIN-2020-10'
            AND subset = 'warc'
            AND url_host_name = 'news.yahoo.co.jp'
            AND url LIKE '%/articles/%'
            AND url NOT LIKE '%/images/%'
            AND fetch_status = 200 )
SELECT *
FROM all_partitions TABLESAMPLE BERNOULLI (50) LIMIT 65000

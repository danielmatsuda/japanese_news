-- Display the most frequent 5000 words in the database snapshot, in descending order.
SELECT Item.word.S AS word, SUM(CAST(Item.count.N AS int)) AS count
FROM news_words
GROUP BY 1  -- group by with an alias not allowed by Presto
ORDER BY 2 DESC
LIMIT 5000;
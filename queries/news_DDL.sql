-- Make a table for the JSON results imported via DynamoDB.
CREATE EXTERNAL TABLE IF NOT EXISTS news_words (
  Item struct <`uuid`:struct<S:string>,
              `count`:struct<N:string>,
              `word`:struct<S:string>>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://danielmatsuda-cc-bucket/AWSDynamoDB/01624324388769-e3edbe60/data/'
TBLPROPERTIES ( 'has_encrypted_data'='true') -- exported table is S3 encrypted by default
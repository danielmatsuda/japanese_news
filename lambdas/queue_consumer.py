import boto3
from botocore.exceptions import ClientError
from io import BytesIO
from gzip import GzipFile
from bs4 import BeautifulSoup
from bs4 import SoupStrainer
import html
import regex
from sudachipy import tokenizer
from sudachipy import dictionary
import uuid


# to package in a lambda layer: bs4, regex, sudachipy


def get_record_data(record, columns):
    """
    Takes in a queue message containing CSV row data. Returns a dictionary containing the row data.
    """
    params_dict = {}
    for column in columns:
        # column strings are encased in double strings, i.e. "'example'"
        column = column[1:-1]
        params_dict[column] = record['messageAttributes'][column]['stringValue']

    return params_dict


def extract_warc(params_dict):
    """
    Takes in a dictionary of row data for an article. Requests and returns the article's WARC extract from a
    Common Crawl bucket.
    """
    client = boto3.client('s3')
    try:
        end_byte = str(int(params_dict['warc_record_offset']) + int(params_dict['warc_record_length']) - 1)
        gzipped_response = client.get_object(
            Bucket='commoncrawl',
            Key=params_dict['warc_filename'],
            Range='bytes=' + params_dict['warc_record_offset'] + '-' + end_byte
        )
        # unzip the file returned
        # Acknowledgement: The following 2 lines of code, originally written by user veselosky, were taken from:
        # https://gist.github.com/veselosky/9427faa38cee75cd8e27
        bytestream = BytesIO(gzipped_response['Body'].read())  # read the StreamingBody object
        warc_extract = GzipFile(None, 'rb', fileobj=bytestream).read().decode('utf-8')
        return warc_extract
    except Exception as e:
        print(e)
        raise e


def get_text(warc):
    """
    Takes in a WARC file and returns a string containing the Yahoo News Japan article title and text (no non-language
    symbols).
    """
    # grab the article title and body text within the WARC file
    article_html_filter = SoupStrainer('article')
    article_html = BeautifulSoup(warc, 'html.parser', parse_only=article_html_filter)
    try:
        article_text = str(article_html.select('header > h1')[0].string)  # gets the article title
    except IndexError:
        article_text = ""  # unable to locate article title -- wasn't found in the usual spot

    article_body = article_html.select('div.article_body p')
    for tag in article_body:  # concatenates the article body text
        article_text += tag.get_text()
    # remove non-language symbols and roman letters
    article_text = html.unescape(article_text)
    article_text = regex.sub(r'[^\p{L}]|[a-zA-Z]', '', article_text)

    return article_text


def get_tokens(article_text):
    """
    Takes a string of Japanese text and returns a list of token strings, excluding particles and auxiliary verbs.
    """
    tokenizer_obj = dictionary.Dictionary(dict_type='core').create()
    mode = tokenizer.Tokenizer.SplitMode.B  # medium-granularity tokenization, see docs for examples
    tokens = [m for m in tokenizer_obj.tokenize(article_text, mode)]
    # remove particles and auxiliary verbs from the list of tokens
    tokens = [m.dictionary_form() for m in tokens if '助詞' not in m.part_of_speech() and '助動詞' not in m.part_of_speech()]

    return tokens


def count_tokens(tokens):
    """
    Takes in a list of token strings. Returns a dictionary, where the keys are unique words, and the
    values are the total number of times the corresponding key was found in the token list.
    """
    counts = {}
    for word in tokens:
        try:
            counts[word] += 1
        except KeyError:
            counts[word] = 1

    return counts


def update_table(row_dict, table_name):
    """
    Insert each key-value pair in row_dict into the table as a new row.
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    for key in row_dict:
        try:
            response = table.put_item(
                Item={
                    # primary key in the table is 'uuid' of String type
                    'uuid': str(uuid.uuid4()),
                    'word': key,
                    'count': row_dict[key]
                }
            )
        except ClientError as e:
            print(e)


def lambda_handler(event, context):
    # unpack the list of columns
    columns_string = event['Records'][0]['messageAttributes']['column_list']['stringValue']
    columns = columns_string[1:-1].split(', ')
    # for each message in the batch, find article word counts
    all_tokens = []
    for record in event['Records']:
        record_data = get_record_data(record, columns)
        warc = extract_warc(record_data)
        text = get_text(warc)
        article_tokens = get_tokens(text)
        all_tokens += article_tokens

    # find the aggregate word count across all articles in the batch, and update the word_storage table
    word_counts = count_tokens(all_tokens)
    print(word_counts)
    update_table(word_counts, 'word_storage')  # table to write to is called 'word_storage'

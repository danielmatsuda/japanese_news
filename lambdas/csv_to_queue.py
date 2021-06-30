import boto3
import urllib


def send_message(sqs, queue_url, column_list, row):
    """
    Takes in a row from the queried CSV file and sends its content as a
    queue message to the queue_url.
    """
    # generate fields for MessageAttributes
    attributes = {}
    attributes['column_list'] = {
        'DataType': 'String',
        'StringValue': str(column_list)
    }
    for i in range(len(column_list)):
        attributes[column_list[i]] = {
            'DataType': 'String',
            'StringValue': row[i]
        }
    try:
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageAttributes=attributes,
            MessageBody=('An article row.')
        )
    except Exception as e:
        print(e)
        raise e


def lambda_handler(event, context):
    """
    Read the CSV file sent by the S3 event notification. Then, send each row of the CSV to an SQS queue.
    """
    # locate the S3 bucket name and (URL encoded) file name in the event notification JSON
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_name = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket_name, Key=file_name)
        sqs = boto3.client('sqs')
        queue_url = 'https://sqs.us-east-1.amazonaws.com/804952628403/article_queue'

        # send messages
        first_line = True
        for row in response['Body'].iter_lines():  # yields raw bytes
            if first_line is False:
                # decode bytes, remove column quote marks for JSON formatting, and convert to list
                row = row.decode('utf-8').replace('"', '')
                row = row.split(',')
                send_message(sqs, queue_url, column_list, row)
            else:
                column_list = row.decode('utf-8').replace('"', '')
                column_list = column_list.split(',')
                first_line = False
    except Exception as e:
        print(e)
        raise e

import json
import boto3
import os
import mysql.connector


def lambda_handler(event, context):
    # TODO implement
    print(event)
    for record in event['Records']:
        payload = record["body"]
        mail_body= (str(payload))
    print(mail_body)
    send_email(mail_body)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

def send_email(data):
    ses_client = boto3.client("ses", region_name="ap-south-1")
    CHARSET = "UTF-8"
    response = ses_client.send_email(
        Destination={
            "ToAddresses": [
                "trainingaws81@gmail.com",
            ],
        },
        Message={
            "Body": {
                "Text": {
                    "Charset": CHARSET,
                    "Data": data,
                }
            },
            "Subject": {
                "Charset": CHARSET,
                "Data": "Report Generated",
            },
        },
        Source="trainingaws81@gmail.com",
    )

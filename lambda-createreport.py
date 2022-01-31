import json
import sys
import pandas as pd
import boto3
import os
import mysql.connector

lambda_client = boto3.client('lambda', region_name='ap-south-1')
s3 = boto3.client('s3')
sqs_client = boto3.client('sqs', region_name="ap-south-1")
sqs_queue_url = 'https://sqs.ap-south-1.amazonaws.com/755369902217/salesreport'
print(sqs_queue_url)
    
my_connection=mysql.connector.connect(
    host = os.environ['host'],
    user= os.environ['user'],
    database = os.environ['database'],
    passwd= os.environ['passwd']
)
cursor = my_connection.cursor()


def lambda_handler(event, context):
    # TODO implement
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        name = record['s3']['object']['key']
    
    name = name.replace('+',' ')
    data_obj = s3.get_object(Bucket=bucket, Key=name);
    data = data_obj['Body'].read()
    salesdata = pd.read_excel(data)
    salesdata['Month'] = salesdata['Order_Date'].dt.strftime('%Y-%m')
    salesdata['Year'] = salesdata['Order_Date'].dt.strftime('%Y')
    try:
        region_monthly = pd.DataFrame(salesdata.groupby(['Region','Month']).sum()[['Total_Revenue','Total_Cost','Total_Profit']].reset_index())
    except:
        print("Failed to create monthly report")  
    monthlyreport(region_monthly)
    try:
        region_annual = pd.DataFrame(salesdata.groupby(['Region','Year']).sum()[['Total_Revenue','Total_Cost','Total_Profit']].reset_index())
    except:
        print("Failed to create annual report") 
    annualreport(region_annual)
    try:
        units_sold = pd.DataFrame(salesdata.groupby(['Region','Year','Sales_Channel']).sum()[['Units_Sold']].reset_index())
    except:
        print("Failed to create unitwise report") 
    unitreport(units_sold)
    # lambda_client.invoke(FunctionName = 'sestest', InvocationType = 'RequestResponse', Payload = json.dumps("All three reports created"))
    return

def monthlyreport(data):
    cursor.execute('CREATE TABLE IF NOT EXISTS monthlyreport (region varchar(100), yearmonth varchar(30), totalsales float, totalrevenue float, totalprofit float)')
    for index,row in data.iterrows():
        monthly_insert = ("INSERT INTO monthlyreport (region,yearmonth,totalsales,totalrevenue,totalprofit) values (%s,%s,%s,%s,%s)")
        val = (row.Region, row.Month, row.Total_Revenue, row.Total_Cost, row.Total_Profit)
        cursor.execute(monthly_insert, val)
    my_connection.commit()
    print("monthly data inserted")
    return

def annualreport(data):
    cursor.execute('CREATE TABLE IF NOT EXISTS annualreport (region varchar(100), year varchar(30), totalsales float, totalrevenue float, totalprofit float)')
    for index,row in data.iterrows():
        annual_insert = ("INSERT INTO annualreport (region,year,totalsales,totalrevenue,totalprofit) values (%s,%s,%s,%s,%s)")
        val = (row.Region, row.Year, row.Total_Revenue, row.Total_Cost, row.Total_Profit)
        cursor.execute(annual_insert, val)
    my_connection.commit()
    print("annual data inserted")
    # try:
    #     sqs_client.send_message(QueueUrl=sqs_queue_url,MessageBody=json.dumps("AnnualReport Generated Successfully"))
    # except:
    #     print("falied to send msg")
    return

def unitreport(data):
    cursor.execute('CREATE TABLE IF NOT EXISTS unitsold (region varchar(100), year varchar(30), saleschannel varchar(30), unitsold float)')
    for index,row in data.iterrows():
        units_statement = ("INSERT INTO unitsold (region,year,saleschannel,unitsold) values (%s,%s,%s,%s)")
        val = (row.Region, row.Year, row.Sales_Channel, row.Units_Sold)
        cursor.execute(units_statement, val)
    my_connection.commit()
    print("units inserted")
    my_connection.close()
    # try:
    #     sqs_client.send_message(QueueUrl=sqs_queue_url,MessageBody=json.dumps("Unitssold Generated Successfully"))
    # except:
    #     print("falied to send msg")
    return
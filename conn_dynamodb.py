import json
import boto3
from copy import deepcopy
from datetime import datetime

client = boto3.client('dynamodb')

def is_valid_date(reg_date, start, end):
  is_valid = False
  if start and end:
    if reg_date >= start and reg_date <= end:
      is_valid = True
  elif start and not end:
    if reg_date >= start:
      is_valid = True
  elif end and not start:
    if reg_date <= end:
      is_valid = True  
  return is_valid
    

def lambda_handler(event, context):    
    field = None
    
    if event['queryStringParameters']:
      key = event['queryStringParameters'].get('keyword')
      value = event['queryStringParameters'].get('text')
      start = event['queryStringParameters'].get('startDate')
      end = event['queryStringParameters'].get('endDate')
      
      
      if key == '디바이스ID': 
        field = 'device_id'
        index = 'device_id-index'
      elif key == '파일명': 
        field = 'original_file'
        index = 'original_file-index'
      elif key == '골프장': 
        field = 'golf_field'
        index = 'golf_field-index'
      elif key == '오류 상태': 
        field = 'error_status'
        index = 'error_status-index'
      
      if not field or not value:
        data = client.scan(
          TableName=''         
        )
    
      else:
        data = client.query(
          TableName='',
          IndexName=index,
          KeyConditionExpression='#name = :value',
          ExpressionAttributeValues={
            ':value': {
              'S': value
            },
          },
          ExpressionAttributeNames={
            '#name': field
          }
        )
        
      if start:
        start = datetime.strptime(start, '%Y-%m-%d')
        
      if end:  
        end = datetime.strptime(end, '%Y-%m-%d')
      
      if start or end:
        print('filtering...')
        new_data = deepcopy(data)
        for d in new_data['Items']:
          reg_date = datetime.strptime(d['registered_date']['S'], '%Y-%m-%d')
          if not is_valid_date(reg_date, start, end):
            data['Items'].remove(d)
        
    else:
        data = client.scan(
          TableName=''          
        )
      
    response = {
      'statusCode': 200,
      'body': json.dumps(data),
      'headers': {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
      },
    }
  
    return response
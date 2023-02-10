import json
import boto3
import base64

client = boto3.client('dynamodb')
s3 = boto3.client('s3')

def lambda_handler(event, context):
    obj_id = event['pathParameters']['id']
    if event['queryStringParameters']:
      path = event['queryStringParameters'].get('path')
      name = path.split('/')[-1]      
    
    else:
      obj = client.get_item(
        TableName='',
        Key={
          "id": {
              "N": str(obj_id)
              }    
          }
      )
      path = obj['Item']['original_path']['S']
      name = obj['Item']['original_file']['S']    
        
    fileObj = s3.get_object(Bucket='superfind', Key=path)
    file_content = fileObj["Body"].read()
    response = {
      "statusCode": 200,
      "headers": {
          "Content-Type": "application/jpg",
          "Content-Disposition": "attachment; filename={}".format(name),
          'Access-Control-Allow-Origin': '*'
      },
      "body": base64.b64encode(file_content),
      "isBase64Encoded": True
    }
  
    return response
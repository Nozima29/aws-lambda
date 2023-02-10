import json
import boto3
import json
from db import put_data

s3 = boto3.client('s3')

def date_to_str(date):
    str_date = date.strftime("%Y-%m-%d")
    return str_date

def fill_obj(folders):
    data = {}
    for folder in folders['Contents']:
        if folder['Key'].endswith('.jpg'):
            size = folder['Size']
            reg_date = date_to_str(folder['LastModified'])
            path = folder['Key']
            name = path.split('/')[-1].strip()
            status = path.split('/')[-2].strip()
            device_id = path.split('/')[-3].strip()
            
            if status == 'original':
                if name in data:
                    data[name].update({'original_path': path})
                    data[name].update({'origin_file_size': size})
                else:
                    data[name] = {}
                    data[name].update({'origin_file_size': size})
                    data[name].update({'reg_date': reg_date})
                    data[name].update({'original_path': path})
                    data[name].update({'device_id': device_id})
                    
            elif status == 'success':
                if name in data:
                    data[name].update({'converted_path': path})
                    data[name].update({'conv_file_size': size})
                    data[name].update({'error_status': status})
                else:
                    data[name] = {}
                    data[name].update({'conv_file_size': size})
                    data[name].update({'reg_date': reg_date})
                    data[name].update({'converted_path': path})
                    data[name].update({'error_status': status})
                    data[name].update({'device_id': device_id})
            
            elif status == 'unsuccess':
                if name in data:
                    data[name].update({'converted_path': path})
                    data[name].update({'conv_file_size': size})
                    data[name].update({'error_status': status})
                else:
                    data[name] = {}
                    data[name].update({'conv_file_size': size})
                    data[name].update({'reg_date': reg_date})
                    data[name].update({'converted_path': path})
                    data[name].update({'error_status': status})
                    data[name].update({'device_id': device_id})
    return data


def lambda_handler(event, context):
    folders = s3.list_objects(Bucket='superfind')
    data = fill_obj(folders)
    response = {}
    if data:
        response = put_data(data)
    
    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }

import boto3

dynamodb = boto3.resource('dynamodb')

def put_data(data_dict):
    table = dynamodb.Table('')
    d_id = 1
    for name, item in data_dict.items():
        if item.get('original_path') and item.get('converted_path'):
            response = table.put_item(
               Item={
                    'id': d_id,
                    'conv_file_size': item.get('conv_file_size'),
                    'converted_file': name,
                    'converted_path': item.get('converted_path'),
                    'device_id': item.get('device_id'),
                    'error_status': item.get('error_status'),
                    'origin_file_size': item.get('origin_file_size'),
                    'original_file': name,
                    'original_path': item.get('original_path'),
                    'rate': item.get('error_status'),
                    'registered_date': item.get('reg_date'),
                    
                    'convH': '10 cm',
                    'convW': '10 cm',
                    'originH': '10 cm',
                    'originW': '10 cm',
                    'error_cause': ' ',
                    'flagH': '5 cm',
                    'flagW': '5 cm',
                    'golf_field': 'Unknown',
                    'memo': ' '
                }
            )
            d_id += 1
    
    return response

    
    
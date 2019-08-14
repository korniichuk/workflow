# Name: workflow-preprocess-json
# Version: 0.1a1

import json


def lambda_handler(event, context):

    result = {}

    try:
        pass
    except BaseException as e:
        print(e)
        raise e
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }

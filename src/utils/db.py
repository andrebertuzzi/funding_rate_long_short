import boto3
import json

client = boto3.client('lambda')


class DataBase:
    @classmethod
    def query(self, sql, args):
        payLoad = {
            'command': sql,
            'args': args
        }
        try:
            response = client.invoke(
                FunctionName='db',
                InvocationType='RequestResponse',
                LogType='None',
                Payload=json.dumps(payLoad),
            )
            response_parsed = json.loads(response['Payload'].read().decode("utf-8"))
            if(isinstance(response_parsed, dict) and response_parsed.get('errorType') == 'error'):
                raise Exception(response_parsed.get('errorMessage'))
            else:
                return response_parsed
        except Exception as e:
            print(e)

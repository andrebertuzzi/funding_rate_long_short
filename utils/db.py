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
        print(payLoad)
        response = client.invoke(
            FunctionName='db',
            InvocationType='RequestResponse',
            LogType='None',
            Payload=json.dumps(payLoad),
        )
        return json.loads(response['Payload'].read().decode("utf-8"))

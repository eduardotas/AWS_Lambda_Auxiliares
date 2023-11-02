import json
import boto3
import logging
from utils import constants

class SecretManager:

    def __init__(self, secret_name) -> None:
        self.__secret_name = secret_name
        self.__session_client = self.__get_session_client()

    def get(self):
        return self.__session_client

    def get_secret(self, key):
        return self.__session_client[key]

    def __get_session_client(self):
        try:
            session = boto3.session.Session()
            region = session.region_name

            client = session.client(
                    service_name=constants.secrets_service_name,
                    region_name=region
                )

            get_secret_value_response = client.get_secret_value(SecretId=self.__secret_name)    
            secret = get_secret_value_response['SecretString']
            secret_json = json.loads(secret)
            return secret_json
        except Exception as e:
            logging.error('Erro in s3_get_secret')
            logging.critical(e, exc_info=True)
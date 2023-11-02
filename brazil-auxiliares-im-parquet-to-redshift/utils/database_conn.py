import boto3
import psycopg2
from utils.constants import (rs_secret_name)
from utils.secret_manager import SecretManager

class DataBaseConn:

    def __init__(self) -> None:
        self.aws_secret = SecretManager(rs_secret_name).get()

    # Função para abrir conexão com o banco de dados;
    def get_connection(self):

        # executa conexão com o banco de dados;    
        conn = psycopg2.connect(
                dbname = self.aws_secret['database'], 
                host = self.aws_secret['hostname'], 
                port = self.aws_secret['port'], 
                user = self.aws_secret['username'], 
                password = self.aws_secret['password'] 
            )  
        return conn
import json
import boto3
import psycopg2
import pyodbc
import urllib.parse
from utils import constants

## Busca credenciais do Secret Manager
def get_secret(secret_name):
    try:
        session = boto3.session.Session()
        region_name = session.region_name
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )
        ger_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = ger_secret_value_response['SecretString']
        secret_json = json.loads(secret)
        secret_json['password_encoded'] = urllib.parse.quote_plus(secret_json['password'])
        return secret_json 
    except Exception as e:
        print(f'Error in getting secret: {e}')

## Cria conexão com Redshift e SQL Server
def get_connection(conn):
    try:
        if conn == 'redshift':
            secret = get_secret(constants.SECRET_NAME_RS)
            conn = psycopg2.connect(
                dbname = secret['database'],
                host = secret['hostname'],
                port = secret['port'],
                user = secret['username'],
                password = secret['password'])
            return conn
        elif conn == 'sql':
            secret = get_secret(constants.SECRET_NAME_SQL)
            conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+secret['host']+';DATABASE='+secret['dbname']+';UID='+secret['username']+';PWD='+ secret['password'])
            return conn
    except Exception as e:
        print(f'Error in establishing connection: {e}')

## Executa query e retorna resultado se houver
def execute_query(conn, query):
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        row = cursor.fetchone()
        return row[0]
    except pyodbc.ProgrammingError as e:
        pass
    except Exception as e:
        print(f'Error executing query: {e}')
    finally:
        cursor.close()
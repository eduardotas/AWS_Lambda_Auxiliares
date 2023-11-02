import sqlalchemy
import pandas as pd
from urllib.parse import quote
from datetime import datetime
from utils import (s3_utils, constants)
from utils.logs_aws_health import LogAWSHealth
from utils.secret_manager import SecretManager

def process(s3, file_key):

    logger = LogAWSHealth()

    ## Capturando as chaves secretas do RedShift
    rs_secrets = SecretManager(constants.rs_secret_name).get()
    redshift = {
        'username': rs_secrets['username'],
        'password': rs_secrets['password'],
        'database': rs_secrets['database'],
        'hostname': rs_secrets['hostname'],
        'port': rs_secrets['port'],
        'schema': constants.rs_schema,
        'table': 'aux_estrut_bricks_sector_tb'
    }

    try:
        ## Download do arquivo parquet do S3
        filename = file_key.split('/')[len(file_key.split('/')) - 1]
        temp_parquet_location = s3_utils.download_s3_file(s3, filename, file_key)

        ## Abrindo arquivo em Pandas DataFrame
        df = pd.read_parquet(temp_parquet_location)
        df['load_data'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        print('Abrindo conexão com banco')
        engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{redshift['username']}:%s@{redshift['hostname']}:{redshift['port']}/{redshift['database']}"  % quote(redshift['password']))

        print('creating table')
        ## Criando a tabela
        sql = f'CREATE TABLE IF NOT EXISTS {constants.rs_schema}.{redshift["table"]} (brick_cod VARCHAR(100), set_id VARCHAR(150), "cluster" VARCHAR(50), factor NUMERIC(10,9), load_data TIMESTAMP, unt_desc VARCHAR(50), cluster_desc VARCHAR(50))'
        engine.execute(sql)

        print('truncate table')
        ## Removendo tabela
        sql = f'TRUNCATE TABLE {constants.rs_schema}.{redshift["table"]}'
        engine.execute(sql)
        
        print(df.columns)
        
        ## Salvando dados
        df.to_sql(redshift["table"], engine, schema=redshift['schema'], index=False, chunksize=19999, method='multi', if_exists='append')
        
        ## Arquivar arquivo parquet
        s3_utils.archive_file(s3, 'setor_brick', filename, file_key)

        ## Limpando pasta temporária
        s3_utils.flush_temp_folder()

        logger.success('Concluído o processo do arquivo parquet, {table} recebeu novos dados.'.format(table=redshift["table"]), len(df))
    except Exception as e:
        logger.error('Erro ao processar a tabela {table}: {e}'.format(e=str(e), table=redshift["table"]))
        raise Exception(e)
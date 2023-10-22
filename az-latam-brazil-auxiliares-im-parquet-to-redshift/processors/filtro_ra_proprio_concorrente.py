from datetime import datetime
import sqlalchemy
import pandas as pd
from urllib.parse import quote
from utils import (s3_utils, constants)
from utils.logs_aws_health import LogAWSHealth
from utils.secret_manager import SecretManager

def process(s3, file_key):

    ## Instanciando classe de Logs
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
        'table': 'aux_filtro_ra_proprio_concorrente'
    }

    try:
        ## Download do arquivo parquet do S3
        filename = file_key.split('/')[len(file_key.split('/')) - 1]
        temp_parquet_location = s3_utils.download_s3_file(s3, filename, file_key)

        ## Abrindo arquivo em Pandas DataFrame
        df = pd.read_parquet(temp_parquet_location)
        df['load_data'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        ## Criando a conexão com banco de dados
        engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{redshift['username']}:%s@{redshift['hostname']}:{redshift['port']}/{redshift['database']}"  % quote(redshift['password']))

        ## Criando a tabela
        sql = f'CREATE TABLE IF NOT EXISTS {constants.rs_schema}.{redshift["table"]} (mercado varchar(100), grupo varchar(100), proprio_concorrente varchar(100), load_data timestamp)'
        engine.execute(sql)
        
        print(df.columns)
        
        ## Limpando a tabela
        sql = f'TRUNCATE TABLE {constants.rs_schema}.{redshift["table"]}'
        engine.execute(sql)

        ## Salvando dados
        df.to_sql(redshift['table'], engine, schema=redshift['schema'], index=False, chunksize=19999, method='multi', if_exists='append')
        
        ## Arquivar arquivo parquet
        s3_utils.archive_file(s3, 'filtro_ra_proprio_concorrente', filename, file_key)

        ## Limpando pasta temporária
        s3_utils.flush_temp_folder()

        logger.success('Concluído o processo do arquivo parquet, {table} recebeu novos dados.'.format(table=redshift['table']), len(df))
    except Exception as e:
        logger.error('Erro ao processar a tabela {table}: {e}'.format(e=str(e), table=redshift["table"]))
        raise Exception(e)
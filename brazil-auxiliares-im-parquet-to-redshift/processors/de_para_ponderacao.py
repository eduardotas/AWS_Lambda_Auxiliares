import datetime
import traceback
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
        'table': 'aux_de_para_ponderacao_tb'
    }

    try:
        ## Download do arquivo parquet do S3
        filename = file_key.split('/')[len(file_key.split('/')) - 1]
        print('Iniciando '+filename)
        temp_parquet_location = s3_utils.download_s3_file(s3, filename, file_key)

        ## Abrindo arquivo em Pandas DataFrame
        df = pd.read_parquet(temp_parquet_location)
        df['load_date'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        ## Criando a conexão com banco de dados
        engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{redshift['username']}:%s@{redshift['hostname']}:{redshift['port']}/{redshift['database']}"  % quote(redshift['password']))

        ## Criando a tabela
        sql = f"""
        CREATE TABLE IF NOT EXISTS {constants.rs_schema}.{redshift["table"]}
            (
            		fcc_id VARCHAR(256)   ENCODE lzo
                	,prod_desc VARCHAR(256)   ENCODE lzo
                	,pack_desc_ims VARCHAR(256)   ENCODE lzo
                	,linha VARCHAR(256)   ENCODE lzo
                	,ponderacao DOUBLE PRECISION   ENCODE RAW
                	,indicacao VARCHAR(256)   ENCODE lzo
                	,load_date VARCHAR(256)   ENCODE lzo
            )
        """
        engine.execute(sql)

        ## Limpando a tabela
        sql = f'TRUNCATE TABLE {constants.rs_schema}.{redshift["table"]}'
        engine.execute(sql)

        ## Salvando dados
        df.to_sql(redshift['table'], engine, schema=redshift['schema'], index=False, chunksize=19999, method='multi', if_exists='append')
        
        ## Tratativas
        sql = f"""
            update {constants.rs_schema}.{redshift["table"]}
            set fcc_id = lpad(fcc_id ,10,0)
        """
        engine.execute(sql)
        
        ## Arquivar arquivo parquet
        s3_utils.archive_file(s3, 'de_para_ponderacao', filename, file_key)

        ## Limpando pasta temporária
        s3_utils.flush_temp_folder()

        logger.success('Concluído o processo do arquivo parquet, {table} recebeu novos dados.'.format(table=redshift['table']), len(df))
    except Exception as e:
        # traceback.print_exception()
        print(str(e))
        logger.error('Erro ao processar a tabela {table}: {e}'.format(e=str(e), table=redshift['table'])) 
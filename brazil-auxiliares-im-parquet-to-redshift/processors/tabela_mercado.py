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
        'table': 'aux_tabela_mercado_v3_tb'
    }

    try:
        ## Download do arquivo parquet do S3
        filename = file_key.split('/')[len(file_key.split('/')) - 1]
        print('Iniciando '+filename)
        temp_parquet_location = s3_utils.download_s3_file(s3, filename, file_key)

        ## Abrindo arquivo em Pandas DataFrame
        df = pd.read_parquet(temp_parquet_location)
        df['load_data'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        ## Criando a conexão com banco de dados
        engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{redshift['username']}:%s@{redshift['hostname']}:{redshift['port']}/{redshift['database']}"  % quote(redshift['password']))

        # Criando a tabela
        sql = f"""
        CREATE TABLE IF NOT EXISTS {constants.rs_schema}.{redshift["table"]}
        (
        	market_id VARCHAR(256)   ENCODE lzo
        	,market_desc VARCHAR(256)   ENCODE lzo
        	,familia VARCHAR(256)   ENCODE lzo
        	,fcc_id VARCHAR(256)   ENCODE lzo
        	,prodcode VARCHAR(256)   ENCODE lzo
        	,prod_desc VARCHAR(256)   ENCODE lzo
        	,pack_desc_ims VARCHAR(256)   ENCODE lzo
        	,cup_id VARCHAR(256)   ENCODE lzo
        	,fator_dot DOUBLE PRECISION   ENCODE RAW
        	,premiacao VARCHAR(256)   ENCODE lzo
        	,unidade_negocio VARCHAR(256)   ENCODE lzo
        	,compliance_rate DOUBLE PRECISION   ENCODE RAW
        	,fator_asma DOUBLE PRECISION   ENCODE RAW
        	,fator_dpoc DOUBLE PRECISION   ENCODE RAW
        	,fator_outros DOUBLE PRECISION   ENCODE RAW
        	,asma_compliance_rate BIGINT   ENCODE az64
        	,dpoc_compliance_rate BIGINT   ENCODE az64
        	,promoted VARCHAR(256)   ENCODE lzo
        	,mft VARCHAR(256)   ENCODE lzo
        	,pws VARCHAR(256)   ENCODE lzo
        	,fator_caixa_padrao DOUBLE PRECISION   ENCODE RAW
        	,load_data VARCHAR(256)   ENCODE lzo
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
        sql_2 = f"""
            update {constants.rs_schema}.{redshift["table"]}
            set prodcode = lpad(prodcode ,5,0)
        """
        engine.execute(sql)
        engine.execute(sql_2)
        
        ## Arquivar arquivo parquet
        s3_utils.archive_file(s3, 'tabela_mercado', filename, file_key)

        ## Limpando pasta temporária
        s3_utils.flush_temp_folder()

        logger.success('Concluído o processo do arquivo parquet, {table} recebeu novos dados.'.format(table=redshift['table']), len(df))
    except Exception as e:
        # traceback.print_exception()
        print(str(e))
        logger.error('Erro ao processar a tabela {table}: {e}'.format(e=str(e), table=redshift['table'])) 
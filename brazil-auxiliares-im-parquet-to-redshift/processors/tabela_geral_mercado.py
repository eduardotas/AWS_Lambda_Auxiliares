import datetime
import traceback
import sqlalchemy
import pandas as pd
from urllib.parse import quote
from utils import (s3_utils, constants)
from utils.logs_aws_health import LogAWSHealth
from utils.secret_manager import SecretManager

def process(s3, file_key):
    hist_folder_name = 'tabela_geral_mercado'
    hist_file_name = 'tabela_geral_mercado_hist.parquet'
    temp_copy_file_name = 'tabela_geral_mercado_final.parquet'
    ## Instanciando classe de Logs
    logger = LogAWSHealth()

    s3_secrets = s3_utils.get_s3_secrets()
    ## Capturando as chaves secretas do RedShift
    rs_secrets = SecretManager(constants.rs_secret_name).get()
    redshift = {
        'username': rs_secrets['username'],
        'password': rs_secrets['password'],
        'database': rs_secrets['database'],
        'hostname': rs_secrets['hostname'],
        'port': rs_secrets['port'],
        'schema': 'br_gdsdata_ads',
        'table': 'azb_aux_mercado_geral'
    }

    try:
        ## Download do arquivo parquet do S3
        filename = file_key.split('/')[len(file_key.split('/')) - 1]
        print('Iniciando '+filename)
        temp_parquet_location = s3_utils.download_s3_file(s3, filename, file_key)

        ## Abrindo arquivo em Pandas DataFrame
        df = pd.read_parquet(temp_parquet_location)

        ## Gerar arquivo historico   
        s3_utils.archive_hist_file(s3, df, hist_folder_name, hist_file_name,file_key)
        
        ### Tratativas dos dados ###
        print('Formatando Arquivo.')
        columns_to_drop = [col for col in df.columns if 'unnamed' in col.lower()]
        df = df.drop(columns=columns_to_drop)

        # Remove os espaços no inicio e fim das colunas que são strings
        for coluna in df.columns:
            if df[coluna].dtype == 'object':
                df[coluna] = df[coluna].str.strip()
        
        df['data_atualizacao'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')           
        df['fator_caixa_padrao'] = df['fator_caixa_padrao'].str.replace('-', '0').str.replace('.','').str.replace(',','.').astype(float)     
        df['premiacao'] = df['premiacao'].replace({'SIM': True, 'NAO': False,'-': False})
        df['mft'] = df['mft'].replace({'SIM': True, 'NAO': False,'-': False})
        df['mdtr'] = df['mdtr'].replace({'SIM': True, 'NAO': False,'-': False})
        df['ddd'] = df['ddd'].replace({'SIM': True, 'NAO': False,'-': False})
        df['gps'] = df['gps'].replace({'SIM': True, 'NAO': False,'-': False})
        df['nps'] = df['nps'].replace({'SIM': True, 'NAO': False,'-': False})
        df['fmb'] = df['fmb'].replace({'SIM': True, 'NAO': False,'-': False})
        df['fmb_nrc'] = df['fmb_nrc'].replace({'SIM': True, 'NAO': False,'-': False})     

        df['linha'] = df['linha'].str.split(',')
        df = df.explode('linha', ignore_index=True)
        df['linha'].fillna('0', inplace=True)
        df['linha'] = df['linha'].astype(int)
                
        df_new_column_order = ['market_id','market_desc','familia','fcc_id','prodcode','prod_desc','pack_desc_ims','fator_dot','fator_caixa_padrao','premiacao','unidade_negocio','mft','linha','mdtr','ddd','gps','nps','fmb','fmb_nrc','data_atualizacao']
        df = df[df_new_column_order]

        df.to_parquet(f'{constants.temp_folder}/{temp_copy_file_name}', index=False)
        print('Fazendo upload para o s3.')
        s3.upload_file(f'{constants.temp_folder}/{temp_copy_file_name}', constants.bucket_staging_name, f'{constants.temp_copy_files_folder}/{temp_copy_file_name}')
                
        ## Criando a conexão com banco de dados
        engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{redshift['username']}:%s@{redshift['hostname']}:{redshift['port']}/{redshift['database']}"  % quote(redshift['password']))

        # Criando a tabela
        sql = f"""
        CREATE TABLE IF NOT EXISTS {redshift["schema"]}.{redshift["table"]}
        (            
            id_mercado bigint,
            descricao_mercado varchar(255),
            familia varchar(255),
            id_produto bigint,
            codigo_produto bigint,
            descricao_produto varchar(255),
            descricao_produto_ims varchar(255),
            fator_dot float8,
            fator_caixa_padrao float8,
            fl_premiacao boolean,
            unidade_negocio varchar(255),
            fl_mft boolean,
            id_linha bigint,
            fl_mdtr boolean,
            fl_ddd boolean,
            fl_gps boolean,
            fl_nps boolean,
            fl_fmb boolean,
            fl_fmb_nrc boolean,            
            data_atualizacao varchar(255)
        );

        TRUNCATE TABLE {redshift["schema"]}.{redshift["table"]};

        COPY {redshift["schema"]}.{redshift["table"]}
            FROM 's3://{constants.bucket_staging_name}/{constants.temp_copy_files_folder}/{temp_copy_file_name}'
            access_key_id '{s3_secrets['access_key_id']}'
            secret_access_key '{s3_secrets['secret_access_key']}'
            FORMAT AS PARQUET;
        """        
        engine.execute(sql)
        
        ## Limpando pasta temporária
        s3_utils.flush_temp_folder()

        logger.success('Concluído o processo do arquivo parquet, {table} recebeu novos dados.'.format(table=redshift['table']), len(df))
    except Exception as e:        
        print(str(e))
        logger.error('Erro ao processar a tabela {table}: {e}'.format(e=str(e), table=redshift['table'])) 
import traceback
import pandas as pd
from utils import (s3_utils, constants, data_utils)
from utils.logs_aws_health import LogAWSHealth

def process(s3, file_key):
    logger = LogAWSHealth()
    try:
        filename = file_key.split('/')[len(file_key.split('/')) - 1]
        temp_csv_location = s3_utils.download_s3_file(s3, filename, file_key)

        # Leitura flat file
        df = pd.read_csv(temp_csv_location, delimiter=';', thousands='.' ,decimal=',', dtype={"MARKET_ID":"string", "FCC_ID":"string", "PRODCODE":"string"})
        
        # Validação de dados
        df = data_utils.trim_columns(df)
        data_utils.is_column_not_null(df, 'FCC_ID', 'FATOR_DOT')
        df.columns = df.columns.str.lower()
        
        # Renomear Coluna
        df = df.rename(columns={'fator_caixa_padrão': 'fator_caixa_padrao'})

        
        temp_parquet_filename = f"{filename[0:filename.index('.csv')]}.parquet"
        temp_parquet_location = f"{constants.temp_folder}/{temp_parquet_filename}"
        
        ## Redefinindo os tipos das colunas
        df[df.columns[0]]  = df[df.columns[0]].astype("string")
        df[df.columns[1]]  = df[df.columns[1]].astype("string")
        df[df.columns[2]]  = df[df.columns[2]].astype("string")
        df[df.columns[3]]  = df[df.columns[3]].astype("string")
        df[df.columns[4]]  = df[df.columns[4]].astype("string")
        df[df.columns[5]]  = df[df.columns[5]].astype("string")
        df[df.columns[6]]  = df[df.columns[6]].astype("string")
        df[df.columns[7]]  = df[df.columns[7]].astype("string")
        df[df.columns[8]]  = df[df.columns[8]].astype(float)
        df[df.columns[9]]  = df[df.columns[9]].astype("string")
        df[df.columns[10]] = df[df.columns[10]].astype("string")
        df[df.columns[11]] = df[df.columns[11]].astype(float)
        df[df.columns[12]] = df[df.columns[12]].astype(float)
        df[df.columns[13]] = df[df.columns[13]].astype(float)
        df[df.columns[14]] = df[df.columns[14]].astype(float)
        df[df.columns[15]] = df[df.columns[15]].astype(float).fillna(0).astype(int)
        df[df.columns[16]] = df[df.columns[16]].astype(float).fillna(0).astype(int)
        df[df.columns[17]] = df[df.columns[17]].astype("string")
        df[df.columns[18]] = df[df.columns[18]].astype("string")
        df[df.columns[19]] = df[df.columns[19]].astype("string")
        df[df.columns[20]] = df[df.columns[20]].astype(float)

        ## Salvando para parquet
        df.to_parquet(temp_parquet_location)

        s3_utils.upload_file(temp_parquet_filename)

        s3_utils.archive_file(s3, 'tabela_mercado', filename, file_key)

        s3_utils.flush_temp_folder()

        logger.success('Arquivo {file} convertido em parquet e salvo em STG'.format(file=file_key), len(df))
    except Exception as e:
        traceback.print_exc()
        logger.error('Erro ao processar arquivo {file}'.format(file=file_key))
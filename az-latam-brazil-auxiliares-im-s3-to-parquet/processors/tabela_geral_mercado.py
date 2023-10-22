import traceback
import pandas as pd
from utils import (s3_utils, constants, data_utils)
from utils.logs_aws_health import LogAWSHealth
from datetime import datetime

def process(s3, file_key):
    logger = LogAWSHealth()
    try:
        filename = file_key.split('/')[len(file_key.split('/')) - 1]
        file_format = '.'+filename.split('.')[1]
        temp_csv_location = s3_utils.download_s3_file(s3, filename, file_key)

        # Leitura flat file
        df = pd.read_csv(temp_csv_location, delimiter=';', thousands='.' ,decimal=',')  
        df.columns = df.columns.str.lower()
        df.columns = df.columns.str.strip()
        
        # Renomear Coluna
        df = df.rename(columns={'fmb+nrc': 'fmb_nrc','fator_caixa_padrão': 'fator_caixa_padrao'})

        #Renomear o arquivo        
        temp_parquet_filename = f"{filename[0:filename.index(file_format)]}.parquet"
        temp_parquet_location = f"{constants.temp_folder}/{temp_parquet_filename}"
        date_now = datetime.now().strftime("%Y%m%d%H%M")
        filename = f"{filename[0:filename.index(file_format)]}_{date_now}{file_format}"

        ## Salvando para parquet
        df.to_parquet(temp_parquet_location)

        s3_utils.upload_file(temp_parquet_filename)

        s3_utils.archive_file(s3, 'tabela_geral_mercado', filename, file_key)

        s3_utils.flush_temp_folder()

        logger.success('Arquivo {file} convertido em parquet e salvo em STG'.format(file=file_key), len(df))
    except Exception as e:
        traceback.print_exc()
        logger.error('Erro ao processar arquivo {file}'.format(file=file_key))
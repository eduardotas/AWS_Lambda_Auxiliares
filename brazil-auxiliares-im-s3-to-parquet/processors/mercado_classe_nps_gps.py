import traceback
import pandas as pd
from utils import (s3_utils, constants)
from utils.logs_aws_health import LogAWSHealth

def process(s3, file_key):
    logger = LogAWSHealth()
    try:
        filename = file_key.split('/')[len(file_key.split('/')) - 1]
        temp_csv_location = s3_utils.download_s3_file(s3, filename, file_key)

        df = pd.read_csv(temp_csv_location, delimiter=';', decimal=',')
        df.columns = df.columns.str.lower()
        df = df.rename(columns={'attribute_value': 'bu', 'product_id': 'pack_cd'})
        
        temp_parquet_filename = f"{filename[0:filename.index('.csv')]}.parquet"
        temp_parquet_location = f"{constants.temp_folder}/{temp_parquet_filename}"
        
        ## Redefinindo os tipos das colunas
        df.columns = df.columns.astype(str)

        ## Salvando para parquet
        df.to_parquet(temp_parquet_location)

        s3_utils.upload_file(temp_parquet_filename)

        s3_utils.archive_file(s3, 'mercado_classe', filename, file_key)

        s3_utils.flush_temp_folder()

        logger.success('Arquivo {file} convertido em parquet e salvo em STG'.format(file=file_key), len(df))
    except Exception as e:
        traceback.print_exc()
        logger.error('Erro ao processar arquivo {file}'.format(file=file_key))
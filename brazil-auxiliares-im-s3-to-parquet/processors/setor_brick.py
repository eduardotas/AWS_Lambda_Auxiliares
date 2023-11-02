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
        df.rename(columns={'brick_id':'brick_cod'}, inplace=True)

        print(constants.temp_folder, filename)
        temp_parquet_filename = f"{filename[0:filename.index('.csv')].lower()}.parquet"
        temp_parquet_location = f"{constants.temp_folder}/{temp_parquet_filename}"
        
        ## Redefinindo os tipos das colunas
        df['unt_desc'] = df['unt_desc'].astype("string") 
        df['set_id'] = df['set_id'].astype("string")
        df['brick_cod'] = df['brick_cod'].astype("string")
        df['cluster'] = df['cluster'].astype("string")
        df['cluster_desc'] = df['cluster_desc'].astype("string")
        df['factor'] = df['factor'].astype(float)
        
        ## Salvando para parquet
        df.to_parquet(temp_parquet_location)

        s3_utils.upload_file(temp_parquet_filename)

        s3_utils.archive_file(s3, 'setor_brick', filename, file_key)

        s3_utils.flush_temp_folder()

        logger.success('Arquivo {file} convertido em parquet e salvo em STG'.format(file=file_key), len(df))
    except Exception as e:
        traceback.print_exc()
        logger.error('Erro ao processar arquivo {file}'.format(file=file_key))
import os
from datetime import datetime

### Environment ###
env = os.getenv('env')
env = env.lower() if env is not None else 'dev'

### AWS S3 ###
s3_secret_name='s3-prod-secret-name'

### Secret Manager
secrets_service_name='secretsmanager'

### DIRETÓRIOS DO S3 ###
temp_folder = '/tmp'
bucket_raw_name = 's3-raw-bucket'
bucket_staging_name = 's3-staging-bucket'
bucket_folder = 'IM/AUX_OPERACAO_MERCADO'
    
### AWS HEALTH ###
logs_service = 'LAMBDA'
logs_obj_identifier = 'az-latam-brazil-auxiliares-im-s3-to-parquet'
logs_date_time = datetime.now()
logs_origin_att_date = datetime.now()

### Redshift
rs_secret_name=f'redshift-secret-name{env}'

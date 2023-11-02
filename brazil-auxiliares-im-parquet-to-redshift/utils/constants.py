import os
from datetime import datetime

### Environment ###
env = os.getenv('env')
env = env.lower() if env is not None else 'dev'

### AWS S3 ###
s3_secret_name = 's3-secret-dev' if env != 'prod' else 's3-secret-prod'

### Secret Manager
secrets_service_name='secretsmanager'

### DIRETÓRIOS DO S3 ###
temp_folder = '/tmp'
bucket_staging_name = 'bucket-staging-dev' if env != 'prod' else 'bucket-staging-prod' 
bucket_folder = 'IM/AUX_TABLES'
temp_copy_files_folder = 'IM/TEMP'
    
### AWS HEALTH ###
logs_service = 'LAMBDA'
logs_obj_identifier = 'brazil-auxiliares-im-parquet-to-redshift'
logs_date_time = datetime.now()
logs_origin_att_date = datetime.now()

### Redshift
rs_secret_name=f'secret-redshift-{env}'
rs_schema = 'db_schema'

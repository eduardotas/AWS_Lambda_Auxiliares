import os
from datetime import datetime

### Environment ###
env = os.getenv('env')
env = env.lower() if env is not None else 'dev'

### AWS S3 ###
s3_secret_name = 'az-latam-brazil-sm-s3-dev' if env != 'prod' else 'az-latam-brazil-sm-s3-prod'

### Secret Manager
secrets_service_name='secretsmanager'

### DIRETÓRIOS DO S3 ###
temp_folder = '/tmp'
bucket_staging_name = 'az-latam-brazil-staging' if env != 'prod' else 'az-latam-brazil-staging-prod' 
bucket_folder = 'IM/AUX_TABLES'
temp_copy_files_folder = 'IM/TEMP'
    
### AWS HEALTH ###
logs_service = 'LAMBDA'
logs_obj_identifier = 'az-latam-brazil-auxiliares-im-parquet-to-redshift'
logs_date_time = datetime.now()
logs_origin_att_date = datetime.now()

### Redshift
rs_secret_name=f'az-latam-brazil-sm-redshift-ds-{env}'
rs_schema = 'br_gdsdata_ads'

import os
import boto3
from utils import constants
from utils.secret_manager import SecretManager

def get_s3_client():
    s3_secrets = SecretManager(constants.s3_secret_name).get()
    return boto3.client(
        's3',
        aws_access_key_id=s3_secrets['access_key_id'],
        aws_secret_access_key=s3_secrets['secret_access_key']
    )

def download_s3_file(s3, filename, file_key):
    print('Downloading ', file_key)
    temp_file_location = f'{constants.temp_folder}/{filename}'
    s3.download_file(
        constants.bucket_raw_name,
        file_key,
        temp_file_location
    )
    print(f'Finished downloading: ', file_key)
    return temp_file_location

def upload_file(filename):
    print('Uploading file `{file}` to AWS S3 staging'.format(file=filename))
    s3 = get_s3_client()
    with open(f'{constants.temp_folder}/{filename}', 'rb') as file:
        s3.put_object(
            Bucket=constants.bucket_staging_name,
            Body=file,
            Key=f'{constants.bucket_folder}/{filename}'
        )

def archive_file(s3, dir, filename, file_key):
    print('Achiving ', file_key)
    new_key = f'{constants.bucket_folder}/archived/{dir}/{filename}'
    s3.copy_object(Bucket=constants.bucket_raw_name, CopySource={'Bucket': constants.bucket_raw_name, 'Key':file_key}, Key=new_key)
    delete_file(s3, file_key)

def delete_file(s3, file_key):
    print('Deleting ', file_key)
    s3.delete_object(Bucket=constants.bucket_raw_name, Key=file_key)

def flush_temp_folder():
    print('Flushing temporary folder')
    try:
        for f in os.listdir(constants.temp_folder):
            os.remove(os.path.join(constants.temp_folder, f))
    except:
        print('Fail to flush temp folder. CONTINUE!')
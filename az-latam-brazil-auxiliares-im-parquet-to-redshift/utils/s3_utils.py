import os
import boto3
import pandas as pd
from utils import constants
from utils.secret_manager import SecretManager

def get_s3_secrets():
    s3_secrets = SecretManager(constants.s3_secret_name).get()
    return s3_secrets

def get_s3_client():
    s3_secrets = get_s3_secrets()
    return boto3.client(
        's3',
        aws_access_key_id=s3_secrets['access_key_id'],
        aws_secret_access_key=s3_secrets['secret_access_key']
    )
    
def download_s3_file(s3, filename, file_key):
    print('Downloading ', file_key)
    temp_file_location = f'{constants.temp_folder}/{filename}'
    s3.download_file(
        constants.bucket_staging_name,
        file_key,
        temp_file_location
    )
    print(f'Finished downloading: ', file_key)
    return temp_file_location

def archive_file(s3, dir, filename, file_key):
    print('Achiving ', file_key)
    new_key = f'{constants.bucket_folder}/archived/{dir}/{filename}'
    s3.copy_object(Bucket=constants.bucket_staging_name, CopySource={'Bucket': constants.bucket_staging_name, 'Key':file_key}, Key=new_key)
    delete_file(s3, file_key)

def archive_hist_file(s3, df, dir, filename,file_key):
    print('Building history file: ', filename)
    hist_file_key = f'{constants.bucket_folder}/archived/{dir}/{filename}'
    try:
        if s3.list_objects_v2(Bucket=constants.bucket_staging_name, Prefix=hist_file_key)['KeyCount'] > 0: # Se o Arquivo hist existir
            temp_hist_parquet_location = download_s3_file(s3, filename, hist_file_key) # Download arquivo historico.parquet
            df_hist = pd.read_parquet(temp_hist_parquet_location) # Le o hitorico.parquet
            concatenated_df = pd.concat([df,df_hist], ignore_index=True) # Add os novos dados no hist.parquet
            concatenated_df.to_parquet(temp_hist_parquet_location, index=False) # Cria o arquivo novamente na local temp
            s3.upload_file(temp_hist_parquet_location, constants.bucket_staging_name, hist_file_key) # Envia o historico atualizado para o s3.
        else:
            df.to_parquet(f'{constants.temp_folder}/{filename}', index=False)
            s3.upload_file(f'{constants.temp_folder}/{filename}', constants.bucket_staging_name, hist_file_key)
        delete_file(s3, file_key)
    except Exception as e:
        print('Error while processing historical file.')    

def delete_file(s3, file_key):
    print('Deleting ', file_key)
    s3.delete_object(Bucket=constants.bucket_staging_name, Key=file_key)

def flush_temp_folder():
    print('Flushing temporary folder')
    try:
        for f in os.listdir(constants.temp_folder):
            os.remove(os.path.join(constants.temp_folder, f))
    except:
        print('Fail to flush temp folder. CONTINUE!')


# class utils:
        
#     def download_s3_file_new(self,prefixo):
                            
#         s3_raw = boto3.client('s3',aws_access_key_id = self.aws_access_key_id_raw , aws_secret_access_key = self.aws_secret_access_key_raw)
#         files_raw = s3_raw.list_objects(Bucket=self.bucket_name_raw,Prefix=self.bucket_folder_name_raw)

#         s3_stg = boto3.client('s3',aws_access_key_id = self.aws_access_key_id_stg , aws_secret_access_key = self.aws_secret_access_key_stg)
#         files_stg = s3_stg.list_objects(Bucket=self.bucket_name_stg,Prefix=self.bucket_folder_name_stg)

#         files_to_upload = []
#         raw_files_s3 = []
#         stg_files_s3 = []

#         #array name files RAW
#         for f in files_raw["Contents"]:
#             #f["Key"] = f["Key"].upper()
#             if f["Key"].find(".csv") != -1 and f["Key"].find(prefixo) != -1 :
#                 diretorio = f["Key"][0:f["Key"].find(f["Key"].split('/')[len(f["Key"].split('/'))-1])]
#                 raw_files_s3.append([f["Key"].split('/')[len(f["Key"].split('/'))-1],diretorio])

#         #array name files STG
#         for f in files_stg["Contents"]:
#             #f["Key"] = f["Key"].upper()
#             if f["Key"].find(".parquet") != -1 and f["Key"].find(prefixo) != -1 :
#                 diretorio = f["Key"][0:f["Key"].find(f["Key"].split('/')[len(f["Key"].split('/'))-1])]
#                 stg_files_s3.append([f["Key"].split('/')[len(f["Key"].split('/'))-1],diretorio])

#         #eliminate files equals
#         if len(stg_files_s3) == 0:
#                 files_to_upload = raw_files_s3
#         else:
#             for mv in raw_files_s3:
#                 for n in range(len(stg_files_s3)):
#                     if mv[0][0:len(mv[0])-4].upper() == stg_files_s3[n][0][0:len(stg_files_s3[n][0])-8].upper():
#                         break
#                     if n == len(stg_files_s3) -1:
#                         files_to_upload.append(mv)
        
#         #Save files TMP
#         for f in files_to_upload:
#             self.download_s3_file(f)

#         #Convert files in TMP
#         for f in files_to_upload:
#             self.convert_parquet(f,prefixo)

#         #Uploda parquet to Staging
#         for f in files_to_upload:
#             self.upload(f)
        
#         #Delete All temp
#         self.delete_files()
    
#     def convert_parquet(self,file,prefixo):
        
#         df = pd.DataFrame(pd.read_csv(f'{temp_folder}{file[0]}', sep=';', low_memory=False))
#         df = df.fillna(0)
        
#         if prefixo in 'grade_de_produtos':
#             df[df.columns[0]] = df[df.columns[0]].astype("string")
#             df[df.columns[1]] = df[df.columns[1]].astype(int)
#             df[df.columns[2]] = df[df.columns[2]].astype(int)
#         elif prefixo in 'setor_brick':
#             df[df.columns[0]] = df[df.columns[0]].astype("string")
#             df[df.columns[1]] = df[df.columns[1]].astype("string")
#             df[df.columns[2]] = df[df.columns[2]].astype("string")
#             df[df.columns[3]] = df[df.columns[3]].astype(float)
#         elif prefixo in 'setor_cnpj_acesso':
#             df[df.columns[0]] = df[df.columns[0]].astype(int)
#             df[df.columns[1]] = df[df.columns[1]].astype(int)
#             df[df.columns[2]] = df[df.columns[2]].astype(int)
#             df[df.columns[3]] = df[df.columns[3]].astype("string")
#         elif prefixo in 'tabela_mercado':
#             df[df.columns[0]]  = df[df.columns[0]].astype("string")
#             df[df.columns[1]]  = df[df.columns[1]].astype("string")
#             df[df.columns[2]]  = df[df.columns[2]].astype("string")
#             df[df.columns[3]]  = df[df.columns[3]].astype("string")
#             df[df.columns[4]]  = df[df.columns[4]].astype("string")
#             df[df.columns[5]]  = df[df.columns[5]].astype("string")
#             df[df.columns[6]]  = df[df.columns[6]].astype("string")
#             df[df.columns[7]]  = df[df.columns[7]].astype("string")
#             df[df.columns[8]]  = df[df.columns[8]].astype(float)
#             df[df.columns[9]]  = df[df.columns[9]].astype("string")
#             df[df.columns[10]] = df[df.columns[10]].astype("string")
#             df[df.columns[11]] = df[df.columns[11]].astype(float)
#             df[df.columns[12]] = df[df.columns[12]].astype(float)
#             df[df.columns[13]] = df[df.columns[13]].astype(float)
#             df[df.columns[14]] = df[df.columns[14]].astype(float)
#             df[df.columns[15]] = df[df.columns[15]].astype(int)
#             df[df.columns[16]] = df[df.columns[16]].astype(int)
#             df[df.columns[17]] = df[df.columns[17]].astype("string")
#             df[df.columns[18]] = df[df.columns[18]].astype("string")
#             df[df.columns[19]] = df[df.columns[19]].astype("string")

#         else:
#             df.columns = df.columns.astype(str)
        
#         #convert csv to parquet function
#         df.to_parquet(f'{temp_folder}{file[0][0:len(file[0])-4]}.parquet')
    
#     def upload(self,file):
#         s3_stg = boto3.client('s3',aws_access_key_id = self.aws_access_key_id_stg , aws_secret_access_key = self.aws_secret_access_key_stg) 
        
#         with open(f'{temp_folder}{file[0][0:len(file[0])-4]}.parquet', "rb") as f:
#             s3_stg.put_object(
#                 Bucket=self.bucket_name_stg,
#                 Body=f,
#                 Key=f'{self.bucket_folder_name_stg}/{file[0][0:len(file[0])-4]}.parquet'
#             )
    
#     def delete_files(self):
#         dir = self.temp_folder
#         for f in os.listdir(dir):
#             os.remove(os.path.join(dir, f))
import json
import logging
import urllib.parse
import boto3
from utils import s3_utils
from processors import (setor_brick, aux_state_uf, filtro_ra_proprio_concorrente, 
                        grade_produtos, mercado_classe_nps_gps, painel_hcp,cluster_pdv
                        ,setor_cnpj_acesso, tabela_mercado,tabela_geral_mercado,de_para_ponderacao,asma)

s3 = boto3.client('s3')

def lambda_handler(event, context):
    print('Starting az-latam-brazil-auxiliares-im-parquet-to-redshift')
    print(event)
    # print("Received event: " + json.dumps(event, indent=2))
    # Get the object from the event and show its content type
    # bucket = event['Records'][0]['s3']['bucket']['name']
    items = event['Records']
    # items = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    try:
        s3 = s3_utils.get_s3_client()
        for item in items:
            key = urllib.parse.unquote_plus(item['s3']['object']['key'], encoding='utf-8')
            if 'archived' in key.lower():
                print('Arquivo já arquivado!')
                return { 'statusCode': 204 }
            elif 'aux_uf' in key.lower():
                aux_state_uf.process(s3, key)
            elif 'filtro_ra_proprio_concorrente' in key.lower():
                filtro_ra_proprio_concorrente.process(s3, key)
            elif 'grade_de_produtos' in key.lower():
                grade_produtos.process(s3, key)
            elif 'mercado_classe' in key.lower():
                mercado_classe_nps_gps.process(s3, key)
            elif 'painel_hcp' in key.lower():
                painel_hcp.process(s3, key)
            elif 'pdv_cluster' in key.lower():
                cluster_pdv.process(s3, key)
            elif 'setor_brick' in key.lower():
                setor_brick.process(s3, key)
            elif 'setor_cnpj_acesso' in key.lower():
                setor_cnpj_acesso.process(s3, key)
            elif 'tabela_mercado' in key.lower():
                tabela_mercado.process(s3, key)
            elif 'tabela_geral_mercado' in key.lower():
                tabela_geral_mercado.process(s3, key)   
            elif 'de-para ponderacao' in key.lower():
                de_para_ponderacao.process(s3, key)
            elif 'taxa_asma' in key.lower():
                asma.process(s3, key)                  
            else:
                print('Arquivo {key} ainda não possui processamento implementado.')
                return { 'statusCode': 204 }
    except Exception as e:
        logging.error(e)
        print('Error: ', e)
        return {'statusCode': 400}
    finally:
        print('Finished!')
        return {'statusCode': 200}

def print_header(header_name):
    line = ''.zfill(60).replace('0', '#')
    text = f'## {header_name.upper()}'.ljust(58)+'##'
    print(f"""{line}\n{text}\n{line}""")
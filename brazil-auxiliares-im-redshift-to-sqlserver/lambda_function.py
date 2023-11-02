import urllib.parse
from processors import (grade_produto, mercado_classe, painel_hcp, pdv_cluster, perfil_demanda, setor_brick, setor_cnpj_acesso, tabela_mercado,asma)
from utils.constants import (LOGS_OBJ_IDENTIFIER)

def lambda_handler(event, context):
    print(f'Starting {LOGS_OBJ_IDENTIFIER}')
    print(f'{event=}')
    
    try:
        items = event['requestPayload']['Records']
    except KeyError:
        items = event['requestPayload']['requestPayload']['Records']
    except Exception as e:
        print('ERROR: ', e)
        return {'statusCode': 400}
    
    try:
        for item in items:
            bucket = urllib.parse.unquote_plus(item['s3']['bucket']['name'], encoding='utf-8')
            key = urllib.parse.unquote_plus(item['s3']['object']['key'], encoding='utf-8')
            s3_key = f's3://{bucket}/{key}'
            print_header(s3_key)
            if 'grade_de_produtos' in key.lower():
                grade_produto.process()
            elif 'mercado_classe' in key.lower():
                mercado_classe.process()
            elif 'painel_hcp' in key.lower():
                painel_hcp.process()
            elif 'pdv_cluster' in key.lower():
                pdv_cluster.process()
            elif 'perfil_demanda' in key.lower():
                perfil_demanda.process()
            elif 'setor_brick' in key.lower():
                setor_brick.process()
            elif 'setor_cnpj_acesso' in key.lower():
                setor_cnpj_acesso.process()    
            elif 'tabela_mercado' in key.lower():
                tabela_mercado.process()
            elif 'taxa_asma' in key.lower():
                asma.process()                  
            else:
                print(f'File {s3_key} does not have processor implemented.')
                return {'statusCode': 204}
                
        print('Finished!')
        return {'statusCode': 200}
    except Exception as e:
        print('ERROR: ', e)
        return {'statusCode': 400}

def print_header(header_name):
    line = ''.zfill(80).replace('0', '#')
    text = f'## {header_name.upper()}'.ljust(58)+'##'
    print(f"""{line}\n{text}\n{line}""")
    
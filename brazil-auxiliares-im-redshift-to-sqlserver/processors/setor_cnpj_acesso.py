from utils.connection import (get_connection, execute_query)
from utils.constants import (SQL_DBNAME, SQL_TMP_SETOR_CNPJ_ACESSO, SP_TMP_SETOR_CNPJ_ACESSO)
from utils import logs_aws_health as log

def process():
    ## Cria queries
    sp_query = f'exec {SP_TMP_SETOR_CNPJ_ACESSO}'
    query = f'SELECT COUNT(*) FROM {SQL_DBNAME}.{SQL_TMP_SETOR_CNPJ_ACESSO}'
    
    ## Cria conexão
    conn = get_connection('sql')
    
    ## Executa queries
    execute_query(conn, sp_query)
    result = execute_query(conn, query)
    if result == 0:
        raise
    
    try:
        conn.commit()
        log.insert_log('SUCCESS', f'Stored procedure {SP_TMP_SETOR_CNPJ_ACESSO} executed with success.', result)
    except Exception as e:
        log.insert_log('ERROR', f'Error executing stored procedure {SP_TMP_SETOR_CNPJ_ACESSO}.')
        raise
    finally:
        conn.close()

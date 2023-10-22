from utils.connection import (get_connection, execute_query)
from utils.constants import (SQL_DBNAME, SQL_TMP_TABELA_ASMA, SP_TMP_TABELA_ASMA)
from utils import logs_aws_health as log

def process():
    ## Cria queries
    sp_query = f'exec {SP_TMP_TABELA_ASMA}'
    query = f'SELECT COUNT(*) FROM {SQL_DBNAME}.{SQL_TMP_TABELA_ASMA}'
    
    ## Cria conexão
    conn = get_connection('sql')
    
    ## Executa queries
    execute_query(conn, sp_query)
    result = execute_query(conn, query)
    if result == 0:
        raise
    
    try:
        conn.commit()
        log.insert_log('SUCCESS', f'Stored procedure {SQL_TMP_TABELA_ASMA} executed with success.', result)
    except Exception as e:
        log.insert_log('ERROR', f'Error executing stored procedure {SQL_TMP_TABELA_ASMA}.')
        raise
    finally:
        conn.close()

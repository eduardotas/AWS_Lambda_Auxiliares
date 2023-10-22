from utils.connection import (get_connection, execute_query)
from utils.constants import (SQL_DBNAME, SQL_TAB_MERCADO, SP_TAB_MERCADO, SQL_TMP_TABELA_MERCADO, SP_TMP_TABELA_MERCADO)
from utils import logs_aws_health as log

def process():
    ## Cria queries
    sp_query_mercado = f'exec {SP_TAB_MERCADO}'
    query_mercado = f'SELECT COUNT(*) FROM {SQL_DBNAME}.{SQL_TAB_MERCADO}'
    
    sp_query_tmp_mercado = f'exec {SP_TMP_TABELA_MERCADO}'
    query_tmp_mercado = f'SELECT COUNT(*) FROM {SQL_DBNAME}.{SQL_TMP_TABELA_MERCADO}'
    
    ## Cria conexão
    conn = get_connection('sql')
    
    ## Executa queries
    execute_query(conn, sp_query_mercado)
    result_mercado = execute_query(conn, query_mercado)
    
    execute_query(conn, sp_query_tmp_mercado)
    result_tmp_mercado = execute_query(conn, query_tmp_mercado)
    if result_mercado == 0 or result_tmp_mercado == 0:
        raise
    
    try:
        conn.commit()
        log.insert_log('SUCCESS', f'Stored procedures {SP_TAB_MERCADO} e {SP_TMP_TABELA_MERCADO} executed with success.', result_mercado + result_tmp_mercado)
    except Exception as e:
        log.insert_log('ERROR', f'Error executing stored procedure {SP_TAB_MERCADO} e {SP_TMP_TABELA_MERCADO}.')
        raise
    finally:
        conn.close()

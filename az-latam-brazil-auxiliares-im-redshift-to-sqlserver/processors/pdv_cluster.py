from utils.connection import (get_connection, execute_query)
from utils.constants import (SQL_DBNAME, SQL_PDV_CLUSTER, SP_PDV_CLUSTER, SQL_TMP_PDV_CLUSTER, SP_TMP_PDV_CLUSTER)
from utils import logs_aws_health as log

def process():
    ## Cria queries
    sp_query_pdv = f'exec {SP_PDV_CLUSTER}'
    query_pdv = f'SELECT COUNT(*) FROM {SQL_DBNAME}.{SQL_PDV_CLUSTER}'
    
    sp_query_tmp_pdv = f'exec {SP_TMP_PDV_CLUSTER}'
    query_tmp_pdv = f'SELECT COUNT(*) FROM {SQL_DBNAME}.{SQL_TMP_PDV_CLUSTER}'
    
    ## Cria conexão
    conn = get_connection('sql')
    
    ## Executa queries
    execute_query(conn, sp_query_pdv)
    result_pdv = execute_query(conn, query_pdv)
    
    execute_query(conn, sp_query_tmp_pdv)
    result_tmp_pdv = execute_query(conn, query_tmp_pdv)
    
    if result_pdv == 0 or result_tmp_pdv == 0:
        raise
    
    try:
        conn.commit()
        log.insert_log('SUCCESS', f'Stored procedures {SP_PDV_CLUSTER} e {SP_TMP_PDV_CLUSTER} executed with success.', result_pdv + result_tmp_pdv)
    except Exception as e:
        log.insert_log('ERROR', f'Error executing stored procedure {SP_PDV_CLUSTER} e {SP_TMP_PDV_CLUSTER}.')
        raise
    finally:
        conn.close()
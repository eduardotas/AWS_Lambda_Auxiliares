
from utils import connection
from utils.constants import (LOGS_SERVICE, LOGS_OBJ_IDENTIFIER, LOG_TABLE, LOGS_DATE_TIME, LOGS_ORIGIN_ATT_DATE, RS_SCHEMA)

## Insere logs na tabela aws health
def insert_log(status, log_stream, registers_amout=0):
    print(f'{log_stream=}, {registers_amout=}')
    query = f"""
            INSERT INTO {RS_SCHEMA}.{LOG_TABLE}
            (service, obj_identifier, date_time, status,log_stream, registers_amout, origin_att_date)
            VALUES('{LOGS_SERVICE}','{LOGS_OBJ_IDENTIFIER}','{LOGS_DATE_TIME}','{status}','{log_stream}','{registers_amout}','{LOGS_ORIGIN_ATT_DATE}');
        """
    conn = connection.get_connection('redshift')
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()
    except Exception as e:
        print(f'Error executing log: {e}')
    finally:
        cursor.close()
        conn.close()
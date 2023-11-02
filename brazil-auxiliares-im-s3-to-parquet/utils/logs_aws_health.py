import imp
import logging
from utils.constants import (logs_service, logs_date_time, logs_obj_identifier, logs_origin_att_date)
from utils.database_conn import DataBaseConn

class LogAWSHealth:

    def __init__(self, level=logging.INFO):
        self.logger = logging.getLogger('BRAZIL-AUXILIARES-IM')
        self.logger.setLevel(level)


    def log(self, message, status='SUCCESS', register_amount=0):
        print(self.logger.level, message)
        logging.log(self.logger.level, message)
        conn = DataBaseConn().get_connection()
        curr = conn.cursor()

        sql = f"""
            INSERT INTO TABELA_DE_LOGS
            (service, obj_identifier, date_time, status,log_stream, registers_amout, origin_att_date)
            VALUES('{logs_service}','{logs_obj_identifier}','{logs_date_time}','{status}','{message}','{register_amount}','{logs_origin_att_date}');
        """

        curr.execute(sql)
        conn.commit()
        conn.close()

    def success(self, message, amount=0):
        self.log(message, 'SUCCESS', amount)

    def error(self, message, amount=0):
        self.logger.setLevel(logging.ERROR)
        self.log(message, 'ERROR', amount)

import os
from datetime import datetime

### Connection ###
SECRET_NAME_RS = os.environ.get('SECRET_NAME_RS')
SECRET_NAME_SQL = os.environ.get('SECRET_NAME_SQL')

### Redshift ###
RS_SCHEMA = os.environ.get('RS_SCHEMA')

### SQL SERVER ###
SQL_DBNAME = os.environ.get('SQL_DBNAME')
SQL_PDV_CLUSTER = os.environ.get('SQL_PDV_CLUSTER')
SQL_TAB_MERCADO = os.environ.get('SQL_TAB_MERCADO')
SQL_GRADE_DE_PRODUTOS = os.environ.get('SQL_GRADE_DE_PRODUTOS')
SQL_MERCADO_CLASSE = os.environ.get('SQL_MERCADO_CLASSE')
SQL_PAINEL_HCP = os.environ.get('SQL_PAINEL_HCP')
SQL_TMP_PDV_CLUSTER = os.environ.get('SQL_TMP_PDV_CLUSTER')
SQL_TMP_PERFIL_DEMANDA = os.environ.get('SQL_TMP_PERFIL_DEMANDA')
SQL_TMP_SETOR_BRICK = os.environ.get('SQL_TMP_SETOR_BRICK')
SQL_TMP_SETOR_CNPJ_ACESSO = os.environ.get('SQL_TMP_SETOR_CNPJ_ACESSO')
SQL_TMP_TABELA_MERCADO = os.environ.get('SQL_TMP_TABELA_MERCADO')
SQL_TMP_TABELA_ASMA = os.environ.get('SQL_TMP_TABELA_ASMA')

### SQL STORED PROCEDURES ###
SP_PDV_CLUSTER = ''
SP_TAB_MERCADO = ''
SP_GRADE_DE_PRODUTOS = ''
SP_MERCADO_CLASSE = ''
SP_PAINEL_HCP = ''
SP_TMP_PDV_CLUSTER = ''
SP_TMP_PERFIL_DEMANDA = ''
SP_TMP_SETOR_BRICK = ''
SP_TMP_SETOR_CNPJ_ACESSO = ''
SP_TMP_TABELA_MERCADO = ''
SP_TMP_TABELA_ASMA = ''

### AWS HEALTH ###
LOGS_SERVICE = os.environ.get('LOGS_SERVICE')
LOGS_OBJ_IDENTIFIER = os.environ.get('LOGS_OBJ_IDENTIFIER')
LOG_TABLE = os.environ.get('LOG_TABLE')
LOGS_DATE_TIME = datetime.now()
LOGS_ORIGIN_ATT_DATE = datetime.now()

# import pandas as pd

def is_column_not_null(df, *columns):
    for column in columns:
        if df[column].isna().values.any():
            raise Exception(f'Coluna {column} contém valores nulos')
            
def trim_columns(df):
    return df.rename(columns=lambda x: x.strip())
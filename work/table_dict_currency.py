import pandas as pd
import datetime as dt
from table import Table
class DictCurrency(Table):
    """ Таблица DICT_CURRENCY """
    def __init__(this, etl, schema):
        super().__init__(etl)
        this.clearBeforeLoad = False
        this.deleteDuplicatesFull = False
        this.deleteDuplicatesPK = False
        this.pk = ['currency_cd','effective_from_date']
        #this.pk = ['currency_cd','effective_from_date','effective_to_date']
        this.createCommand = (
                f"""
        CREATE TABLE IF NOT EXISTS {schema}.DICT_CURRENCY (
                currency_cd TEXT not null,
                currency_name TEXT not null,
                effective_from_date DATE not null,
                effective_to_date DATE not null,
                PRIMARY KEY(currency_cd, effective_from_date)
        )
        """
        )
        this.truncateCommand = f"TRUNCATE TABLE {schema}.DICT_CURRENCY"
        this.selectCommand = f"SELECT * FROM {schema}.DICT_CURRENCY"
        this.insertCommand = (
            f"""
            INSERT INTO {schema}.DICT_CURRENCY (currency_cd, currency_name, effective_from_date, effective_to_date) 
            VALUES %s
            """
        )
        this.updateCommand = (
            f"""
            UPDATE {schema}.DICT_CURRENCY
                SET currency_name = %s                                        
                WHERE currency_cd = %s and effective_from_date = %s and effective_to_date = %s
        """   
        )
        this.name = f"{schema}.DICT_CURRENCY"
        this.csvName = "dict_currency\dict_currency.csv"
        this.csvSep = ','
        

    def LoadDataFrameFromCsv(this):
        this.df = pd.read_csv(this.GetCsvFileName(), encoding=this.GetEncoding(), sep=this.csvSep,
                                    dtype={
                                    'currency_cd':'object',
                                    'currency_name':'object',
                                    'effective_from_date':'object',
                                    'effective_to_date':'object'
                                    })
        this.df['effective_from_date'] = this.df['effective_from_date'].apply(lambda x: dt.datetime.strptime(x,'%Y-%m-%d').date() if type(x)==str else pd.NaT)
        this.df['effective_to_date'] = this.df['effective_to_date'].apply(lambda x: dt.datetime.strptime(x,'%Y-%m-%d').date() if type(x)==str else pd.NaT)


    def GetDataFrameFromDB(this):
        this.dfDB = pd.read_sql_query(this.selectCommand, con=this.conn_panda,
                                    dtype={
                                    'currency_cd':'object',
                                    'currency_name':'object'
                                    })


    def TransformListForUpdate(this, records):
        for r in records:
            r[0],r[1],r[2],r[3] = r[1],r[0],r[2],r[3]
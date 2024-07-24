import pandas as pd
from table import Table
class Account(Table):
    """ Таблица DS.MD_ACCOUNT_D """
    def __init__(this, etl):
        super().__init__(etl)
        this.pk = ['data_actual_date', 'account_rk']
        this.createCommand = (
                """
        CREATE TABLE IF NOT EXISTS DS.MD_ACCOUNT_D (
                data_actual_date DATE not null,
                data_actual_end_date DATE not null,
                account_rk BIGINT not null,
                account_number VARCHAR(20) not null,
                char_type CHAR(1) not null,
                currency_rk BIGINT not null,
                currency_code VARCHAR(3) not null,
                PRIMARY KEY(data_actual_date, account_rk)
        )
        """
        )
        this.truncateCommand = "TRUNCATE TABLE DS.MD_ACCOUNT_D"
        this.selectCommand = "SELECT * FROM DS.MD_ACCOUNT_D"
        this.insertCommand = (
            """
            INSERT INTO DS.MD_ACCOUNT_D (data_actual_date, data_actual_end_date, account_rk, account_number, char_type, currency_rk, currency_code) 
            VALUES %s
            """
        )
        this.updateCommand = (
            """
            UPDATE DS.MD_ACCOUNT_D
                SET data_actual_end_date = %s,
                    account_number = %s,
                    char_type = %s,
                    currency_rk = %s,
                    currency_code = %s
                WHERE data_actual_date = %s AND account_rk = %s
        """   
        )
        this.name = "md_account_d"
        this.csvName = "md_account_d.csv"
        
    def LoadDataFrameFromCsv(this):
        this.df = pd.read_csv(this.GetCsvFileName(), sep=';', encoding=this.GetEncoding(),
                                dtype={
                                    'ACCOUNT_RK':'int64',
                                    'ACCOUNT_NUMBER':'object',
                                    'CHAR_TYPE':'object',
                                    'CURRENCY_RK':'int64',
                                    'CURRENCY_CODE':'object'
                                    })
        this.df.columns = map(str.lower, this.df.columns)
        this.df['data_actual_date'] = pd.to_datetime(this.df['data_actual_date'], dayfirst=False)
        this.df['data_actual_end_date'] = pd.to_datetime(this.df['data_actual_end_date'], dayfirst=False)
            

    def GetDataFrameFromDB(this):
        this.dfDB = pd.read_sql_query(this.selectCommand, con=this.conn_panda, 
                                      dtype={
                                          'data_actual_date':'datetime64[ns]',
                                          'data_actual_end_date':'datetime64[ns]'})
            

    def TransformListForUpdate(this, records):
        for r in records:
            r[0],r[1],r[2],r[3],r[4],r[5],r[6] = r[1],r[3],r[4],r[5],r[6],r[0],r[2]
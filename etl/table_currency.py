import pandas as pd
from table import Table
class Currency(Table):
    """ Таблица DS.MD_CURRENCY_D """
    def __init__(this, etl):
        super().__init__(etl)
        this.pk = ['currency_rk', 'data_actual_date']
        this.createCommand = (
        """
        CREATE TABLE IF NOT EXISTS DS.MD_CURRENCY_D (
                currency_rk BIGINT not null,
                data_actual_date DATE not null,
                data_actual_end_date DATE,
                currency_code VARCHAR(3),
                code_iso_char VARCHAR(3),
                PRIMARY KEY(currency_rk, data_actual_date)
        )
        """
        )
        this.truncateCommand = "TRUNCATE TABLE DS.MD_CURRENCY_D"
        this.selectCommand = "SELECT * FROM DS.MD_CURRENCY_D"
        this.insertCommand = (
            """
            INSERT INTO DS.MD_CURRENCY_D (currency_rk, data_actual_date, data_actual_end_date, currency_code, code_iso_char)
            VALUES %s
            """
        )
        this.updateCommand = (
            """
            UPDATE DS.MD_CURRENCY_D
                SET data_actual_end_date = %s,
                    currency_code = %s,
                    code_iso_char = %s
                WHERE currency_rk = %s AND data_actual_date = %s
        """   
        )
        this.name = "md_currency_d"
        this.csvName = "md_currency_d.csv"
        
    def LoadDataFrameFromCsv(this):
        this.df = pd.read_csv(this.GetCsvFileName(), sep=';', encoding=this.GetEncoding(), dtype={
                                    'CURRENCY_RK':'int64',
                                    'CURRENCY_CODE':'object',
                                    'CODE_ISO_CHAR':'object'
                                    })
        this.df.columns = map(str.lower, this.df.columns)        
        this.df['data_actual_date'] = pd.to_datetime(this.df['data_actual_date'], dayfirst=False)
        this.df['data_actual_end_date'] = pd.to_datetime(this.df['data_actual_end_date'], dayfirst=False)
        

    def GetDataFrameFromDB(this):
        this.dfDB = pd.read_sql_query(this.selectCommand, con=this.conn_panda, dtype={'data_actual_date':'datetime64[ns]', 'data_actual_end_date':'datetime64[ns]'})
        

    def TransformListForUpdate(this, records):
        for r in records:
            r[0],r[1],r[2],r[3],r[4] = r[2],r[3],r[4],r[0],r[1]
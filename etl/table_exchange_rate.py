import pandas as pd
from table import Table
class ExchangeRate(Table):
    """ Таблица DS.MD_EXCHANGE_RATE_D """
    def __init__(this, etl):
        super().__init__(etl)
        this.pk = ['data_actual_date', 'currency_rk']
        this.createCommand = (
        """
        CREATE TABLE IF NOT EXISTS DS.MD_EXCHANGE_RATE_D (
                data_actual_date DATE not null,
                data_actual_end_date DATE,
                currency_rk BIGINT not null,
                reduced_cource DOUBLE PRECISION,
                code_iso_num VARCHAR(3),
                PRIMARY KEY(data_actual_date, currency_rk)
        )
        """
        )
        this.truncateCommand = "TRUNCATE TABLE DS.MD_EXCHANGE_RATE_D"
        this.selectCommand = "SELECT * FROM DS.MD_EXCHANGE_RATE_D"
        this.insertCommand = (
            """
            INSERT INTO DS.MD_EXCHANGE_RATE_D (data_actual_date, data_actual_end_date, currency_rk, reduced_cource, code_iso_num)
            VALUES %s
            """
        )
        this.updateCommand = (
            """
            UPDATE DS.MD_EXCHANGE_RATE_D
                SET data_actual_end_date = %s,
                    reduced_cource = %s,
                    code_iso_num = %s
                WHERE data_actual_date = %s AND currency_rk = %s
        """   
        )
        this.name = "md_exchange_rate_d"
        this.csvName = "md_exchange_rate_d.csv"
        

    def LoadDataFrameFromCsv(this):
        this.df = pd.read_csv(this.GetCsvFileName(), sep=';', encoding=this.GetEncoding(), dtype={
                                    'CURRENCY_RK':'int64',
                                    'REDUCED_COURCE':'float64',
                                    'CODE_ISO_NUM':'object'
                                    })
        this.df.columns = map(str.lower, this.df.columns)
        this.df['data_actual_date'] = pd.to_datetime(this.df['data_actual_date'], dayfirst=False)
        this.df['data_actual_end_date'] = pd.to_datetime(this.df['data_actual_end_date'], dayfirst=False)
            

    def GetDataFrameFromDB(this):
        this.dfDB = pd.read_sql_query(this.selectCommand, con=this.conn_panda, dtype={
            'data_actual_date':'datetime64[ns]',
            'data_actual_end_date':'datetime64[ns]'})
            

    def TransformListForUpdate(this, records):
        for r in records:
            r[0],r[1],r[2],r[3],r[4] = r[1],r[3],r[4],r[0],r[2]
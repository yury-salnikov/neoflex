import pandas as pd
from table import Table
class Balance(Table):
    def __init__(this, etl):
        """ Таблица DS.FT_BALANCE_F """
        super().__init__(etl)
        this.pk = ['on_date', 'account_rk']
        this.createCommand = (
        """
        CREATE TABLE IF NOT EXISTS DS.FT_BALANCE_F (
            on_date DATE not null,
            account_rk BIGINT not null,
            currency_rk BIGINT,
            balance_out DOUBLE PRECISION,
            PRIMARY KEY(on_date, account_rk)
        )
        """
        )
        this.truncateCommand = "TRUNCATE TABLE DS.FT_BALANCE_F"
        this.selectCommand = "SELECT * FROM DS.FT_BALANCE_F"
        this.insertCommand = (
            """
            INSERT INTO DS.FT_BALANCE_F (on_date, account_rk, currency_rk, balance_out)
            VALUES %s
            """
        )
        this.updateCommand = (
            """
            UPDATE DS.FT_BALANCE_F
                SET currency_rk = %s,
                    balance_out = %s
                WHERE on_date = %s AND account_rk = %s
        """   
        )
        this.name = "ft_balance_f"
        this.csvName = "ft_balance_f.csv"
        
    def LoadDataFrameFromCsv(this):
        this.df = pd.read_csv(this.GetCsvFileName(), sep=';', encoding=this.GetEncoding(), dtype={
                                    'ACCOUNT_RK':'int64',
                                    'CURRENCY_RK':'int64',
                                    'BALANCE_OUT':'float64'
                                    })
        this.df.columns = map(str.lower, this.df.columns)
        this.df['on_date'] = pd.to_datetime(this.df['on_date'], dayfirst=True)
            

    def GetDataFrameFromDB(this):
        this.dfDB = pd.read_sql_query(this.selectCommand, con=this.conn_panda, dtype=
                                      {
                                          'on_date':'datetime64[ns]'})
            

    def TransformListForUpdate(this, records):
        for r in records:
            r[0],r[1],r[2],r[3] = r[2],r[3],r[0],r[1]
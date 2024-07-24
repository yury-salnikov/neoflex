import pandas as pd
from table import Table
class Posting(Table):
    def __init__(this, etl):
        """ Таблица DS.FT_POSTING_F """
        super().__init__(etl)
        this.clearBeforeLoad = True
        this.createCommand = (
        """ CREATE TABLE IF NOT EXISTS DS.FT_POSTING_F (
                oper_date DATE not null,
                credit_account_rk BIGINT not null,
                debet_account_rk BIGINT not null,
                credit_amount DOUBLE PRECISION,
                debet_amount DOUBLE PRECISION
                )
        """
        )
        this.truncateCommand = "TRUNCATE TABLE DS.FT_POSTING_F"
        this.selectCommand = "SELECT * FROM DS.FT_POSTING_F"
        this.insertCommand = (
            """
            INSERT INTO DS.FT_POSTING_F (oper_date, credit_account_rk, debet_account_rk, credit_amount, debet_amount)
            VALUES %s
            """
        )
        this.name = "ft_posting_f"
        this.csvName = "ft_posting_f.csv"
        

    def LoadDataFrameFromCsv(this):
        this.df = pd.read_csv(this.GetCsvFileName(), sep=';', encoding=this.GetEncoding(), dtype={
                                    'CREDIT_ACCOUNT_RK':'int64',
                                    'DEBET_ACCOUNT_RK':'int64',
                                    'CREDIT_AMOUNT':'float64',
                                    'DEBET_AMOUNT':'float64'
                                    })
        this.df.columns = map(str.lower, this.df.columns)
        this.df['oper_date'] = pd.to_datetime(this.df['oper_date'], dayfirst=True)
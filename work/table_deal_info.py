import pandas as pd
import datetime as dt

from table import Table
class DealInfo(Table):
    """ Таблица DEAL_INFO """
    def __init__(this, etl, schema):
        super().__init__(etl)
        this.clearBeforeLoad = False
        this.deleteDuplicatesFull = False
        this.deleteDuplicatesPK = False
        this.pk = ['deal_rk', 'product_rk', 'effective_from_date']
        this.createCommand = (
               f"""
        CREATE TABLE IF NOT EXISTS {schema}.DEAL_INFO (
                deal_rk BIGINT not null,
                deal_num TEXT,
                deal_name TEXT,
                deal_sum NUMERIC,
                client_rk BIGINT not null,
                account_rk BIGINT not null,
                agreement_rk BIGINT not null,
                deal_start_date DATE,
                department_rk BIGINT,
                product_rk BIGINT,
                deal_type_cd TEXT,
                effective_from_date DATE not null,
                effective_to_date DATE not null,
                PRIMARY KEY(deal_rk, product_rk, effective_from_date)
        )
        """
        )
        this.truncateCommand = f"TRUNCATE TABLE {schema}.DEAL_INFO"
        this.selectCommand = f"SELECT * FROM {schema}.DEAL_INFO"
        this.insertCommand = (
           f"""
            INSERT INTO {schema}.DEAL_INFO (deal_rk, deal_num, deal_name, deal_sum, client_rk, account_rk, 
                agreement_rk, deal_start_date, department_rk, product_rk, deal_type_cd, effective_from_date, effective_to_date) 
            VALUES %s
            """
        )
        this.updateCommand = (
           f"""
            UPDATE {schema}.DEAL_INFO
                SET deal_num = %s,
                    deal_name = %s,
                    deal_sum = %s,
                    client_rk = %s,
                    account_rk = %s,
                    agreement_rk = %s,
                    deal_start_date = %s,
                    department_rk = %s,
                    deal_type_cd = %s,
                    effective_to_date = %s
                WHERE deal_rk = %s AND product_rk = %s AND effective_from_date = %s
        """   
        )
        this.name = f"{schema}.DEAL_INFO"
        this.csvName = "loan_holiday_info/deal_info.csv"
        this.csvSep = ','
        

    def LoadDataFrameFromCsv(this):
        this.df = pd.read_csv(this.GetCsvFileName(), encoding=this.GetEncoding(), sep=this.csvSep,
                                dtype={
                                    'deal_rk':'int64',
                                    'deal_num':'object',
                                    'deal_name':'object',
                                    'deal_sum':'float64',
                                    'client_rk':'int64',
                                    'account_rk':'int64',
                                    'agreement_rk':'int64',
                                    'department_rk':'int64',
                                    'product_rk':'int64',
                                    'deal_type_cd':'object'
                                    })
        this.df['deal_start_date'] = this.df['deal_start_date'].apply(lambda x: dt.datetime.strptime(x,'%Y-%m-%d').date() if type(x)==str else pd.NaT)
        this.df['effective_from_date'] = this.df['effective_from_date'].apply(lambda x: dt.datetime.strptime(x,'%Y-%m-%d').date() if type(x)==str else pd.NaT)
        this.df['effective_to_date'] = this.df['effective_to_date'].apply(lambda x: dt.datetime.strptime(x,'%Y-%m-%d').date() if type(x)==str else pd.NaT)
            

    def GetDataFrameFromDB(this):
        this.dfDB = pd.read_sql_query(this.selectCommand, con=this.conn_panda)
            

    def TransformListForUpdate(this, records):
        for r in records:
            r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7],r[8],r[9],r[10],r[11],r[12] = r[1],r[2],r[3],r[4],r[5],r[6],r[7],r[8],r[10],r[12],r[0],r[9],r[11]
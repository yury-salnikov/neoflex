import pandas as pd
import datetime as dt

from table import Table
class ProductInfo(Table):
    """ Таблица PRODUCT """
    def __init__(this, etl, schema):
        super().__init__(etl)
        this.clearBeforeLoad = True
        this.deleteDuplicatesFull = True 
        this.deleteDuplicatesPK = False
        this.pk = ['product_rk', 'product_name', 'effective_from_date']
        this.createCommand = (
                f"""
        CREATE TABLE IF NOT EXISTS {schema}.PRODUCT (
                product_rk bigint not null,
                product_name text,
                effective_from_date date not null,
                effective_to_date date not null,
                PRIMARY KEY(product_rk, product_name, effective_from_date)
        )
        """
        )
        this.truncateCommand = f"TRUNCATE TABLE {schema}.PRODUCT"
        this.selectCommand = f"SELECT * FROM {schema}.PRODUCT"
        this.insertCommand = (
            f"""
            INSERT INTO {schema}.PRODUCT (product_rk, product_name, effective_from_date, effective_to_date) 
            VALUES %s
            """
        )
        this.updateCommand = (
            f"""
            UPDATE {schema}.PRODUCT
                SET product_name = %s,
                    effective_to_date = %s
                WHERE product_rk = %s AND product_name = %s AND effective_from_date = %s
        """   
        )
        this.name = f"{schema}.PRODUCT"
        this.csvName = "loan_holiday_info/product_info.csv"
        this.csvSep = ','
        

    def LoadDataFrameFromCsv(this):
        this.df = pd.read_csv(this.GetCsvFileName(), encoding=this.GetEncoding(), sep=this.csvSep,
                                dtype={
                                    'product_rk':'int64',
                                    'product_name':'object',
                                    'effective_from_date':'object',
                                    'effective_to_date':'object'
                                    })
        this.df['effective_from_date'] = this.df['effective_from_date'].apply(lambda x: dt.datetime.strptime(x,'%Y-%m-%d').date() if type(x)==str else pd.NaT)
        this.df['effective_to_date'] = this.df['effective_to_date'].apply(lambda x: dt.datetime.strptime(x,'%Y-%m-%d').date() if type(x)==str else pd.NaT)
            

    def GetDataFrameFromDB(this):
        this.dfDB = pd.read_sql_query(this.selectCommand, con=this.conn_panda, 
                                dtype={
                                    'product_rk':'int64',
                                    'product_name':'object',
                                    'effective_from_date':'object',
                                    'effective_to_date':'object'
                                    })
            

    def TransformListForUpdate(this, records):
        for r in records:
            r[0],r[1],r[2],r[3] = r[3],r[0],r[1],r[2]
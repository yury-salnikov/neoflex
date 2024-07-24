import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from chardet.universaldetector import UniversalDetector

class Table:
    def __init__(this, etl):
        """ Базовый класс для таблиц """
        this.etl = etl
        this.conn = etl.conn 
        this.cursor = etl.cursor
        this.conn_panda = etl.conn_panda
        this.csvDirectory = "data"
        this.name = ""
        this.csvName = ""
        this.clearBeforeLoad = False
        this.pk = []
        pd.set_option('display.max_rows', 500)
        pd.set_option('display.max_columns', 500)
        pd.set_option('display.width', 1000)


    def GetCsvFileName(this):
        """ Возвращает полный путь к файлу csv """
        return this.csvDirectory + "/" + this.csvName
    

    def GetEncoding(this):
        """ Возвращает кодировку csv файла """
        fn = this.GetCsvFileName()
        detector = UniversalDetector()
        with open(fn, 'rb') as file:
            for line in file:
                detector.feed(line)
                if detector.done:
                    break
            detector.close()
        enc = detector.result['encoding']
        return enc
        

    def LoadDataFrameFromCsv(this):
        """ Загружает csv файл в python """
        this.df = pd.read_csv(this.GetCsvFileName(), sep=';', encoding=this.GetEncoding())
        this.df.columns = map(str.lower, this.df.columns)
        

    def HasNulls(this):       
        return this.df.isnull().any().any()
    

    def FilterNulls(this):
        this.df = this.df[ this.df.isnull().any(axis='columns') ]
        

    def PrintDF(this):
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print(this.df)    
            

    def GetDataFrameFromDB(this):
        this.dfDB = pd.read_sql_query(this.selectCommand, con=this.conn_panda)    
              

    def InsertData(this, df):
        data = this.GetListFromDF(df)
        execute_values(this.cursor, this.insertCommand, data)
        

    def UpdateData(this, df):
        data = this.GetListFromDF(df)
        this.TransformListForUpdate(data)
        this.cursor.executemany(this.updateCommand, data)
            

    def Clear(this):
        this.cursor.execute(this.truncateCommand)
        

    def GetListFromDF(this, df):   
        df.replace({ np.nan: None }, inplace = True)
        return df.values.tolist()
import time
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from conn import Conn
from chardet.universaldetector import UniversalDetector
from table_account import *
from table_balance import *
from table_currency import *
from table_exchange_rate import *
from table_ledger_account import *
from table_logs import *
from table_posting import *
from vtable_account_balance import *
from vtable_account_turnover import *
from vtable_f101_v1 import *

class ETL(Conn):
    """ Класс для организации процесса загрузки csv файлов в БД """
    def __init__(this, user, password, host, port, database):
        super().__init__(user, password, host, port, database)
        this.dsTables = [Account(this), Balance(this), Currency(this), ExchangeRate(this), LedgerAccount(this), Posting(this)]
        this.dmVTables = [VAccountBalance(this), VAccountTurnover(this), VF101_V1(this)]
        conn = Conn(user, password, host, port, database)
        this.log = Logs(conn)


    def Run(this):
        """ Главная процедура запуска ETL процесса """
        this.log.PrintInfo("Начало загрузки csv файлов")
        time.sleep(5)
        this.log.PrintInfo("Начало создания объектов в БД необходимых для загрузки csv файлов")
        this.CreateTablesIfNotExists()
        this.log.PrintInfo("Конец создания объектов в БД необходимых для загрузки csv файлов")
        this.log.PrintInfo("Начало загрузки csv файлов в python")
        this.LoadFromCsvFiles()
        this.log.PrintInfo("Конец загрузки csv файлов в python")
        this.log.PrintInfo("Начало загрузки таблиц из БД в python для слияния с таблицами из csv")
        this.LoadDataFromDatabase()
        this.log.PrintInfo("Конец загрузки таблиц из БД в python для слияния с таблицами из csv")
        this.log.PrintInfo("Начало загрузки таблиц из python в БД")
        this.LoadDataToDatabase()
        this.log.PrintInfo("Конец загрузки таблиц из python в БД")
        this.log.PrintInfo("Конец загрузки csv файлов")
        

    def CreateTablesIfNotExists(this):
        """ Создание в БД объектов, необходимых для ETL процесса """
        this.cursor.execute("CREATE SCHEMA IF NOT EXISTS DS")  
        this.cursor.execute("CREATE SCHEMA IF NOT EXISTS DM") 
        for table in this.dsTables:
            this.cursor.execute(table.createCommand)
        for vt in this.dmVTables:
            this.cursor.execute(vt.createCommand)
        this.conn.commit()
            

    def LoadFromCsvFiles(this):
        """ Загрузка из файлов csv в python """
        for table in this.dsTables:
            table.LoadDataFrameFromCsv()
            if len(table.pk) > 0:
                table.df = table.df.drop_duplicates(subset=table.pk)

                
    def LoadDataFromDatabase(this):
        """ Загрузка таблиц из БД в python """ 
        for table in this.dsTables:
            if table.clearBeforeLoad:
                continue
            table.GetDataFrameFromDB()


    def LoadDataToDatabase(this):
        """ Загрузка таблиц в БД из python """
        for table in this.dsTables:
            this.LoadTableToDatabase(table)
        this.conn.commit()


    def LoadTableToDatabase(this, t):
        """ Загрузка одной таблицы в БД из python """
        this.log.PrintInfo(f"Начало загрузки таблицы {t.name} из python в БД")
        if t.clearBeforeLoad:
            this.log.PrintInfo("Загрузка выполняется в режиме с полной очисткой таблицы в БД")
            t.Clear()
            t.InsertData(t.df)
            return
        else:
            this.log.PrintInfo("Загрузка выполняется в режиме обновления таблицы в БД")
            pk = t.df[t.pk]
            dfDB = t.dfDB
            df = t.df
            
            dfLeftPK = pd.merge(df, dfDB, how='left', on=t.pk, indicator=True)
            dfNewElems = dfLeftPK[dfLeftPK['_merge'] == 'left_only']
            dfNewElemsPK = dfNewElems[t.pk]
            pkNewFilter = pk.isin(dfNewElemsPK).all(axis='columns')
            dfInsert = t.df[pkNewFilter].copy()            
            t.InsertData(dfInsert)
            this.log.PrintInfo(f"Было добавлено {len(dfInsert.index)} новых строк")
            
            dfLeftFull = pd.merge(df, dfDB, how='left', indicator=True)
            dfNewModifiedElems = dfLeftFull[dfLeftFull['_merge'] == 'left_only']
            dfNewModifiedElemsPK = dfNewModifiedElems[t.pk]
            dfModifiedElemsPK =  dfNewModifiedElemsPK[~dfNewModifiedElemsPK.isin(dfNewElemsPK).all(axis='columns')]
            pkModifiedFilter = pk.isin(dfModifiedElemsPK).all(axis='columns')
            dfUpdate = t.df[pkModifiedFilter].copy()
            t.UpdateData(dfUpdate)
            this.log.PrintInfo(f"Было обновлено {len(dfUpdate.index)} строк")
        this.log.PrintInfo(f"Конец загрузки таблицы {t.name} из python в БД")


def Main():    
    try:
        etl = ETL('neouser','neoflex','localhost','5433','bankdb')
        etl.log.PrintInfo("Подключение к БД успешно установлено")
        etl.Run()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        etl.log.PrintError("Во время загрузки csv файлов произошла ошибка работы с БД, файлы не загружены")
        etl.log.PrintError(error)
    except Exception as ex:
        print(ex) 
        etl.log.PrintError("Во время загрузки csv файлов произошла ошибка, файлы не загружены")
        etl.log.PrintError(ex)
    finally:
        etl.log.PrintInfo("Выполняется закрытие подключения")
        etl.Dispose()


if __name__ == '__main__':
    Main()
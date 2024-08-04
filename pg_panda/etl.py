import time
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import datetime as dt
from conn import Conn
from table_logs import Logs 

class ETL(Conn):
    """ Класс для организации процесса загрузки csv файлов в БД """
    def __init__(this, user, password, host, port, database, schemas=[], syncTables=[], vitrTables=[], logDB = False):
        super().__init__(user, password, host, port, database)
        conn = Conn(user, password, host, port, database)
        this.schemas = schemas
        this.syncTables = [table(this, schema) for schema, table  in syncTables]
        this.vitrTables = [vitr(this) for vitr in vitrTables]
        this.logDB = logDB
        if logDB:
            this.log = Logs(conn)


    def Run(this):
        """ Главная процедура запуска ETL процесса """
        if this.logDB:
            this.log.PrintInfo("Начало ETL")        
            this.log.PrintInfo("Начало создания объектов в БД необходимых для загрузки csv файлов")
        this.CreateDbObjects()
        if this.logDB:
            this.log.PrintInfo("Конец создания объектов в БД необходимых для загрузки csv файлов")
            this.log.PrintInfo("Начало загрузки csv файлов в python")
        this.LoadFromCsvFiles()
        if this.logDB:
            this.log.PrintInfo("Конец загрузки csv файлов в python")
            this.log.PrintInfo("Начало загрузки таблиц из БД в python для слияния с таблицами из csv")           
        this.LoadDataFromDatabase()
        if this.logDB:
            this.log.PrintInfo("Конец загрузки таблиц из БД в python для слияния с таблицами из csv")
            this.log.PrintInfo("Начало загрузки таблиц из python в БД")            
        this.LoadDataToDatabase()
        if this.logDB:
            this.log.PrintInfo("Конец загрузки таблиц из python в БД")
            this.log.PrintInfo("Конец ETL")
        

    def CreateDbObjects(this):
        """ Создание в БД объектов, необходимых для ETL процесса """
        for schema in this.schemas:
            this.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")  
        for table in this.syncTables:
            this.cursor.execute(table.createCommand)
        for vitr in this.vitrTables:
            this.cursor.execute(vitr.createCommand)
        this.conn.commit()
            

    def LoadFromCsvFiles(this):
        """ Загрузка из файлов csv в python """
        for table in this.syncTables:
            print(f"Загрзука таблицы {table.name} из CSV")
            table.LoadDataFrameFromCsv()
            if table.deleteDuplicatesFull:
                duplicated = table.df.duplicated()
                fullDuplicatedCount = duplicated.sum() 
                if fullDuplicatedCount > 0:
                    print(f"Кол-во полных дублей в {table.name} равно {fullDuplicatedCount}")
                    print(table.df[duplicated])
                    table.df = table.df.drop_duplicates()
            if len(table.pk) > 0:
                duplicatedPK = table.df.duplicated(subset=table.pk)
                pkDuplicatedCount = duplicatedPK.sum()
                if pkDuplicatedCount > 0:
                    print(f"Кол-во дублей по первичному ключу в {table.name} равно {pkDuplicatedCount}")
                    print(table.df[duplicatedPK])
                    if table.deleteDuplicatesPK:
                        table.df = table.df.drop_duplicates(subset=table.pk) 
                    else:
                        print("В таблице имеются неудалённые дубли по первичному ключу, исправьте ключ или удалите дубли")                
            print("")
            print("")
        
                
    def LoadDataFromDatabase(this):
        """ Загрузка таблиц из БД в python """ 
        for table in this.syncTables:
            if table.clearBeforeLoad:
                continue
            table.GetDataFrameFromDB()


    def LoadDataToDatabase(this):
        """ Загрузка таблиц в БД из python """
        for table in this.syncTables:
            this.LoadTableToDatabase(table)
        this.conn.commit()


    def LoadTableToDatabase(this, t):
        """ Загрузка одной таблицы в БД из python """
        if this.logDB:            
            this.log.PrintInfo(f"Начало загрузки таблицы {t.name} из python в БД")
        if t.clearBeforeLoad:
            if this.logDB:
                this.log.PrintInfo("Загрузка выполняется в режиме с полной очисткой таблицы в БД")
            t.Clear()
            t.InsertData(t.df)
            if this.logDB:
                this.log.PrintInfo(f"Всего в таблицу {t.name} было загружено {len(t.df.index)} записей")
            return
        else:
            if this.logDB:
                this.log.PrintInfo("Загрузка выполняется в режиме обновления таблицы в БД")
            pk = t.df[t.pk]
            dfDB = t.dfDB
            df = t.df
            dfLeftPK = pd.merge(df, dfDB, how='left', on=t.pk, suffixes=[None,'dfDB'], indicator=True)
            dfNewPKElems = dfLeftPK[dfLeftPK['_merge'] == 'left_only']
            dfInsert = dfNewPKElems[df.columns.to_list()]
            
            dfInsertCopy = dfInsert.copy()       
            print("Данные для вставки")
            print(dfInsertCopy)
            t.InsertData(dfInsertCopy)
            if this.logDB:
                this.log.PrintInfo(f"Было добавлено {len(dfInsertCopy.index)} новых строк")
            
            dfLeftFull = pd.merge(df, dfDB, how='left',suffixes=[None,'dfDB'], indicator=True)
            dfModifiedElems = dfLeftFull[dfLeftFull['_merge'] == 'left_only']
            dfModifiedElems = dfModifiedElems[df.columns.to_list()]

            dfLeftUpdate = pd.merge(dfModifiedElems, dfInsert, how='left', suffixes=[None,'dfInsert'], indicator=True)           
            dfUpdateElems = dfLeftUpdate[dfLeftUpdate['_merge'] == 'left_only']           
            dfUpdate = dfUpdateElems[df.columns.to_list()]
            
            dfUpdateCopy = dfUpdate[df.columns.to_list()].copy()            
            print("Данные для обновления")
            print(dfUpdateCopy)
            t.UpdateData(dfUpdateCopy)
            if this.logDB:
                this.log.PrintInfo(f"Было обновлено {len(dfUpdateCopy.index)} строк")
        if this.logDB:
            this.log.PrintInfo(f"Конец загрузки таблицы {t.name} из python в БД")
        print("")
        print("")
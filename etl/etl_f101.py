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
from vtable_f101_v2 import *

class F101(Conn):
    """ Класс для организации процесса загрузки и выгрузки формы 101 между БД и csv файлом """
    def __init__(this, user, password, host, port, database):
        super().__init__(user, password, host, port, database)
        this.f101v1Table = VF101_V1(this)
        this.f101v2Table = VF101_V2(this)
        conn = Conn(user, password, host, port, database)
        this.log = Logs(conn)
        

    def FromDbToCsv(this):
        """ Выгрузка формы 101 из БД в csv файл """
        this.log.PrintInfo("Начало выгрузки формы 101 в csv файл")
        time.sleep(5)
        this.log.PrintInfo("Начало выгрузки формы 101 в python")
        this.f101v1Table.GetDataFrameFromDB()
        this.log.PrintInfo(f"Получено из базы {len(this.f101v1Table.dfDB.index)} строк")
        this.log.PrintInfo("Конец выгрузки формы 101 в python")
        csvFileName = this.f101v1Table.GetCsvFileName()
        this.log.PrintInfo("Начало выгрузки формы 101 из в python в csv файл")
        this.f101v1Table.dfDB.to_csv(csvFileName, sep=';', index=False, mode='w')
        this.log.PrintInfo("Конец выгрузки формы 101 из в python в csv файл")
        this.log.PrintInfo("Конец выгрузки формы 101 в csv файл")


    def FromCsvToDb(this):
        """ Загрузка формы 101 из csv файла в БД с предварительным созданием и полной очисткой таблицы в БД """
        this.log.PrintInfo("Начало загрузки формы 101 из csv файла")
        time.sleep(5)
        this.cursor.execute(this.f101v2Table.createCommand)
        this.cursor.execute(this.f101v2Table.truncateCommand)
        this.log.PrintInfo("Начало загрузки формы 101 из csv файла в python")
        this.f101v2Table.LoadDataFrameFromCsv()
        this.log.PrintInfo(f"Получено из csv файла {len(this.f101v2Table.df.index)} строк")
        this.log.PrintInfo("Конец загрузки формы 101 из csv файла в python")
        this.log.PrintInfo("Начало загрузки формы 101 из python в БД")
        this.f101v2Table.InsertData(this.f101v2Table.df)
        this.conn.commit()
        this.log.PrintInfo("Конец загрузки формы 101 из python в БД")
        this.log.PrintInfo("Конец загрузки формы 101 из csv файла")


def Main(fromDbToCsv):   
    if fromDbToCsv:
        FromDbToCsv()
    else:
        FromCsvToDB()


def FromDbToCsv():
    try:
        etl = F101('neouser','neoflex','localhost','5433','bankdb')
        etl.log.PrintInfo("Подключение к БД успешно установлено")
        etl.FromDbToCsv()
    except Exception as ex:
        print(ex) 
        etl.log.PrintError("Во время выгрузки в csv файл произошла ошибка, таблица не выгружена")
        etl.log.PrintError(ex)
    finally:
        etl.log.PrintInfo("Выполняется закрытие подключения")
        etl.Dispose()


def FromCsvToDB():
    try:
        etl = F101('neouser','neoflex','localhost','5433','bankdb')
        etl.log.PrintInfo("Подключение к БД успешно установлено")
        etl.FromCsvToDb()
    except Exception as ex:
        print(ex) 
        etl.log.PrintError("Во время загрузки csv файла произошла ошибка, файл csv не загружен")
        etl.log.PrintError(ex)
    finally:
        etl.log.PrintInfo("Выполняется закрытие подключения")
        etl.Dispose()
        

if __name__ == '__main__':
    Main(fromDbToCsv=False)
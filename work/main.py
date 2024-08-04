import time
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from etl import ETL
from chardet.universaldetector import UniversalDetector
from table_deal_info import *
from table_dict_currency import *
from table_product_info import *


def Main():    
    """
    Ошибка парсинга года 2999 в панду - решение добавить  errors='coerce' в метод pandas to datetime
    Ошибка парсинга dict_currency - откуда ошибка непонятно, решение добавить вручную формат format='%Y-%m-%d' в метод pandas to datetime
    Ошибка загрузки product - data в null имеется придётся менять таблицу ограничений...
    """
    try:
        tables = []
        tables.append(['RD',ProductInfo])
        tables.append(['RD',DealInfo])
        #tables.append(['DM',DictCurrency])
        etl = ETL('neouser','neoflex','localhost','5433','dwh',
                    schemas=['RD2', 'DM2'],
                    syncTables=tables,
                    vitrTables=[], logDB=True
                  )
        etl.Run()
    except Exception as ex:
        print(ex) 
    finally:
        etl.Dispose()


if __name__ == '__main__':
    Main()
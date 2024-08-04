import psycopg2
from sqlalchemy import create_engine
import pandas as pd

class Conn:
    """ Отвечает за подключение к БД """
    def __init__(this, user, password, host, port, database):
        this.user = user
        this.password = password
        this.host = host
        this.port = port
        this.database = database
        this.conn = this.GetConnection() 
        this.cursor = this.conn.cursor()
        this.conn_panda = this.GetEngine()
        this.log = None


    def GetConnection(this):
        """ Подключение через psycopg2 """
        conn = psycopg2.connect(f"dbname='{this.database}' user='{this.user}' host='{this.host}' port='{this.port}' password='{this.password}'")
        return conn


    def GetEngine(this):
        """ Подключение через pandas """
        engine = create_engine(url=f"postgresql://{this.user}:{this.password}@{this.host}:{this.port}/{this.database}")
        return engine
    

    def Dispose(this):
        """ Закрывает все подключения """
        this.cursor.close()
        this.conn.close()
        this.conn_panda.dispose()
        if this.log != None:
            this.log.cursor.close()
            this.log.conn.close()
            this.log.conn_panda.dispose()
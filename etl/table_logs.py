import pandas as pd
from sqlalchemy.orm import query
from table import Table
class Logs(Table):
    """ Таблица LOGS.EVENTS """
    def __init__(this, etl):
        super().__init__(etl)
        this.name = "events"
        this.createCommand =  (
        """
        CREATE TABLE IF NOT EXISTS LOGS.EVENTS (
                id bigserial PRIMARY KEY,
                level text not null,
                message text not null,
                event_time timestamp not null default CURRENT_TIMESTAMP
        )
        """
        )
        this.truncateCommand = "TRUNCATE TABLE LOGS.EVENTS"
        this.insertCommand = (
            """
            INSERT INTO LOGS.EVENTS (level, message)
            VALUES (%s, %s)
            """
        )
        this.selectCommand = "SELECT * FROM LOGS.EVENTS"
        this.cursor.execute("CREATE SCHEMA IF NOT EXISTS LOGS")
        this.cursor.execute(this.createCommand)
        this.conn.commit()
    

    def Print(this, level, message):
        data = [(level, str(message))]
        print(data)
        this.cursor.executemany(this.insertCommand, data)
        this.conn.commit()


    def PrintError(this, message):
        this.Print("error", message)
        

    def PrintWarning(this, message):
        this.Print("warning", message)
        

    def PrintInfo(this, message):
        this.Print("info", message)
        
import pandas as pd
from table import Table

class VF101(Table):
    """ Базовый класс формы 101 """
    def __init__(this, etl, table_name):
        super().__init__(etl)
        this.csvName = "f101.csv"
        this.createCommand =  (
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            FROM_DATE DATE,
            TO_DATE	DATE,
            CHAPTER	CHAR(1),
            LEDGER_ACCOUNT	CHAR(5),
            CHARACTERISTIC	CHAR(1),
            BALANCE_IN_RUB	NUMERIC(23,8),
            R_BALANCE_IN_RUB	NUMERIC(23,8),
            BALANCE_IN_VAL	NUMERIC(23,8),
            R_BALANCE_IN_VAL	NUMERIC(23,8),
            BALANCE_IN_TOTAL	NUMERIC(23,8),
            R_BALANCE_IN_TOTAL	NUMERIC(23,8),
            TURN_DEB_RUB	NUMERIC(23,8),
            R_TURN_DEB_RUB	NUMERIC(23,8),
            TURN_DEB_VAL	NUMERIC(23,8),
            R_TURN_DEB_VAL	NUMERIC(23,8),
            TURN_DEB_TOTAL	NUMERIC(23,8),
            R_TURN_DEB_TOTAL	NUMERIC(23,8),
            TURN_CRE_RUB	NUMERIC(23,8),
            R_TURN_CRE_RUB	NUMERIC(23,8),
            TURN_CRE_VAL	NUMERIC(23,8),
            R_TURN_CRE_VAL	NUMERIC(23,8),
            TURN_CRE_TOTAL	NUMERIC(23,8),
            R_TURN_CRE_TOTAL	NUMERIC(23,8),
            BALANCE_OUT_RUB	NUMERIC(23,8),
            R_BALANCE_OUT_RUB	NUMERIC(23,8),
            BALANCE_OUT_VAL	NUMERIC(23,8),
            R_BALANCE_OUT_VAL	NUMERIC(23,8),
            BALANCE_OUT_TOTAL	NUMERIC(23,8),
            R_BALANCE_OUT_TOTAL	NUMERIC(23,8)
        )
        """
        )
        
        this.insertCommand = (
            f"""
            INSERT INTO {table_name} (FROM_DATE, TO_DATE, CHAPTER, LEDGER_ACCOUNT, CHARACTERISTIC, BALANCE_IN_RUB, R_BALANCE_IN_RUB, BALANCE_IN_VAL,
            R_BALANCE_IN_VAL, BALANCE_IN_TOTAL, R_BALANCE_IN_TOTAL, TURN_DEB_RUB, R_TURN_DEB_RUB, TURN_DEB_VAL, R_TURN_DEB_VAL, TURN_DEB_TOTAL,
            R_TURN_DEB_TOTAL, TURN_CRE_RUB, R_TURN_CRE_RUB, TURN_CRE_VAL, R_TURN_CRE_VAL, TURN_CRE_TOTAL, R_TURN_CRE_TOTAL, BALANCE_OUT_RUB,
            R_BALANCE_OUT_RUB, BALANCE_OUT_VAL, R_BALANCE_OUT_VAL, BALANCE_OUT_TOTAL, R_BALANCE_OUT_TOTAL)
            VALUES %s
            """
        )

        this.truncateCommand = f"TRUNCATE TABLE {table_name}"
        this.selectCommand = f"SELECT * FROM {table_name}"
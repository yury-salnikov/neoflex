import pandas as pd
from table import Table
class VAccountTurnover(Table):
    """ Таблица DM.DM_ACCOUNT_TURNOVER_F """
    def __init__(this, etl):
        super().__init__(etl)
        this.name = "dm_account_turnover_f"
        this.createCommand =  (
        """
        CREATE TABLE IF NOT EXISTS DM.DM_ACCOUNT_TURNOVER_F (
            on_date	DATE,
            account_rk	BIGINT,
            credit_amount	NUMERIC(23,8),
            credit_amount_rub	NUMERIC(23,8),
            debet_amount	NUMERIC(23,8),
            debet_amount_rub	NUMERIC(23,8)
        )
        """
        )
        this.truncateCommand = "TRUNCATE TABLE DM.DM_ACCOUNT_TURNOVER_F"
        this.selectCommand = "SELECT * FROM DM.DM_ACCOUNT_TURNOVER_F"
import pandas as pd
from table import Table
class VAccountBalance(Table):
    """ Таблица DM.DM_ACCOUNT_BALANCE_F """
    def __init__(this, etl):
        super().__init__(etl)
        this.name = "dm_account_balance_f"
        this.createCommand =  (
        """
        CREATE TABLE IF NOT EXISTS DM.DM_ACCOUNT_BALANCE_F (
            on_date DATE not null,
            account_rk BIGINT not null,
            balance_out DOUBLE PRECISION,
            balance_out_rub DOUBLE PRECISION
        )
        """
        )
        this.truncateCommand = "TRUNCATE TABLE DM.DM_ACCOUNT_BALANCE_F"
        this.selectCommand = "SELECT * FROM DM.DM_ACCOUNT_BALANCE_F"
import pandas as pd
from table import Table
class LedgerAccount(Table):
    def __init__(this, etl):
        """ Таблица DS.MD_LEDGER_ACCOUNT_S """
        super().__init__(etl)
        this.pk = ['ledger_account', 'start_date']
        this.createCommand = (
        """
        CREATE TABLE IF NOT EXISTS DS.MD_LEDGER_ACCOUNT_S (
                chapter	CHAR(1),
                chapter_name	VARCHAR(16),
                section_number	INTEGER,
                section_name	VARCHAR(22),
                subsection_name	VARCHAR(21),
                ledger1_account	INTEGER,
                ledger1_account_name	VARCHAR(47),
                ledger_account	INTEGER not null,
                ledger_account_name	VARCHAR(153),
                characteristic	CHAR(1),
                is_resident	INTEGER,
                is_reserve	INTEGER,
                is_reserved	INTEGER,
                is_loan	INTEGER,
                is_reserved_assets	INTEGER,
                is_overdue	INTEGER,
                is_interest	INTEGER,
                pair_account	VARCHAR(5),
                start_date	DATE not null,
                end_date	DATE,
                is_rub_only	INTEGER,
                min_term	CHAR(1),
                min_term_measure	CHAR(1),
                max_term	CHAR(1),
                max_term_measure	CHAR(1),
                ledger_acc_full_name_translit	CHAR(1),
                is_revaluation	CHAR(1),
                is_correct	CHAR(1),
                PRIMARY KEY(ledger_account, start_date)
        )
        """
        )
        this.truncateCommand = "TRUNCATE TABLE DS.MD_LEDGER_ACCOUNT_S"
        this.selectCommand = "SELECT * FROM DS.MD_LEDGER_ACCOUNT_S"
        this.insertCommand = (
            """
            INSERT INTO DS.MD_LEDGER_ACCOUNT_S (chapter, chapter_name, section_number, section_name, subsection_name, ledger1_account, ledger1_account_name,
                ledger_account, ledger_account_name, characteristic, start_date, end_date
            )
            VALUES %s
            """
        )
        this.updateCommand = (
            """
            UPDATE DS.MD_LEDGER_ACCOUNT_S
                SET chapter = %s,
                    chapter_name = %s,
                    section_number = %s,
                    section_name = %s,
                    subsection_name = %s,
                    ledger1_account = %s,
                    ledger1_account_name = %s,
                    ledger_account_name = %s,
                    characteristic = %s,
                    end_date = %s
                WHERE ledger_account = %s AND start_date = %s
        """   
        )
        this.name = "md_ledger_account_s"
        this.csvName = "md_ledger_account_s.csv"
        

    def LoadDataFrameFromCsv(this):
        this.df = pd.read_csv(this.GetCsvFileName(), sep=';', encoding=this.GetEncoding(), dtype={
                                    'CHAPTER':'object',
                                    'CHAPTER_NAME':'object',
                                    'SECTION_NUMBER':'int64',
                                    'SECTION_NAME':'object',
                                    'SUBSECTION_NAME':'object',
                                    'LEDGER1_ACCOUNT':'int64',
                                    'LEDGER1_ACCOUNT_NAME':'object',
                                    'LEDGER_ACCOUNT':'int64',
                                    'LEDGER_ACCOUNT_NAME':'object',
                                    'CHARACTERISTIC':'object'
                                    })
        this.df.columns = map(str.lower, this.df.columns)
        this.df['start_date'] = pd.to_datetime(this.df['start_date'], dayfirst=False)
        this.df['end_date'] = pd.to_datetime(this.df['end_date'], dayfirst=False)
            

    def GetDataFrameFromDB(this):
        this.dfDB = pd.read_sql_query(this.selectCommand, con=this.conn_panda, dtype={
            'start_date':'datetime64[ns]',
            'end_date':'datetime64[ns]'})
            

    def TransformListForUpdate(this, records):
        for r in records:
            r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7],r[8],r[9],r[10],r[11] = r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[8],r[9],r[11],r[7],r[10]
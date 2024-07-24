import pandas as pd
from vtable_f101 import VF101

class VF101_V2(VF101):
    """ Таблица 101 вариант для экспорта импорта """
    def __init__(this, etl):
        super().__init__(etl, "DM.DM_F101_ROUND_F_V2")
        this.name = "dm_f101_round_f_v2"
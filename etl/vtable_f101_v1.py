import pandas as pd
from vtable_f101 import VF101

class VF101_V1(VF101):
    """ Таблица 101 основной вариант """
    def __init__(this, etl):
        super().__init__(etl, "DM.DM_F101_ROUND_F")
        this.name = "dm_f101_round_f"
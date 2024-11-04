# %%
import pathlib
import sys
import os
sys.path.insert(1,os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from .helper import *
from utils.measurementreport import MeasureReport

class MR_Helper(Helper):
    def __init__(self, db) -> None:
        super().__init__(db)
        self.db.sql("""
        create table IF NOT EXISTS MR_Table
        (
            "type" VARCHAR,
            "time" timestamp_ns,
            "event" VARCHAR,
            "others" VARCHAR
        );       
        """)
    
    def run(self):
        rrc_df = self.db.sql("select * from RRC_OTA_Packet").df()
        mrs = MeasureReport(rrc_df)
        
        for k, v in mrs.items():
            mr_type = k
            for i in v:
                data = [mr_type]
                data.append(i.time)
                data.append(i.event)
                data.append(i.others)
                cnt = 0
                for element in data:
                    if element != None and element != '':
                        data[cnt] = "\'" + str(element) + "\'"
                    else:
                        data[cnt] = 'NULL'
                    cnt += 1
                data_str = ','.join(data)
                self.db.sql(f"""
                INSERT INTO MR_Table
                VALUES ({data_str});
                """)

#%%
if __name__ == "__main__":
    import duckdb
    db = duckdb.connect('/home/wmnlab/Documents/r12921063/database/2023-03-26_UDP_Bandlock_All_RM500Q_qc00_#02.db')
    ho_helper = MR_Helper(db)
    ho_helper.run()
# %%
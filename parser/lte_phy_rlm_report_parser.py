from .parser import Parser
import xml.etree.ElementTree as ET
import datetime

class LTE_PHY_RLM_Report_Parser(Parser):
    def __init__(self) -> None:
        super().__init__()
        self.columns = {
            "Timestamp": "TIMESTAMP_NS", 
            "Timestamp_BS": "TIMESTAMP_NS",  
            "T310_Timer_Status": "INT",
            "Out_of_Sync_Count": "INT",
            "Frame_Elapse": "INT",
        }
        self.table_name = "LTE_PHY_RLM_Report"
        self.type_id = ["LTE_PHY_RLM_Report"]
        
    def parse_to_db(self, msg, tree, db):
        timestamp = tree.find("pair[@key='device_timestamp']")
        if timestamp != None:
            timestamp = timestamp.text
        else:
            timestamp = tree.find("pair[@key='timestamp']").text
        bs_timestamp = tree.find("pair[@key='timestamp']").text
        records = tree.find("pair[@key='Records']/list")
        t310_status = 0
        out_of_sync_count = 0
        frame_elapse = 0
        for i in records.findall('item'):
            out_of_sync_count = int(i.find("dict/pair[@key='Out of Sync Count']").text)
            t310_status = int(i.find("dict/pair[@key='T310 Timer Status']").text)
            if t310_status == 1:
                frame_elapse += 1
            else:
                frame_elapse = 0
        db.sql(f"""
        INSERT INTO {self.table_name} VALUES (
            '{timestamp}', '{bs_timestamp}', {t310_status}, {out_of_sync_count}, 
            {frame_elapse}
        );
        """)
        return True
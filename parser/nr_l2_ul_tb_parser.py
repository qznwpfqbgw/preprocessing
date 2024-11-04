from .parser import Parser
import xml.etree.ElementTree as ET
import datetime

class NR_L2_UL_TB_Parser(Parser):
    def __init__(self) -> None:
        super().__init__()
        self.columns = {
            "Timestamp": "TIMESTAMP_NS", 
            "Timestamp_BS": "TIMESTAMP_NS",     
            "Grant_Size": "BIGINT"
        }
        self.table_name = "NR_L2_UL_TB"
        self.type_id = ["5G_NR_L2_UL_TB"]
        
    def parse_to_db(self, msg, tree, db):
        timestamp = tree.find("pair[@key='device_timestamp']")
        if timestamp != None:
            timestamp = timestamp.text
        else:
            timestamp = tree.find("pair[@key='timestamp']").text
        bs_timestamp = tree.find("pair[@key='timestamp']").text
        total_grant = 0
        tti_info = tree.find("pair[@key='TTI Info']/list")
        if tti_info != None:
            for i in tti_info.findall('item'):
                tb_info = i.find("dict/pair[@key='TB Info']/list")
                if tb_info != None:
                    for j in tb_info.findall('item'):
                        total_grant += int(j.find("dict/pair[@key='Grant Size']").text)
        db.sql(f"""
            INSERT INTO {self.table_name} VALUES (
                '{timestamp}', '{bs_timestamp}', {total_grant}
            );
        """)
        return True
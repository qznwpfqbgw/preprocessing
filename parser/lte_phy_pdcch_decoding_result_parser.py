from .parser import Parser
import xml.etree.ElementTree as ET
import datetime

class LTE_PHY_PDCCH_Decoding_Result_Parser(Parser):
    def __init__(self) -> None:
        super().__init__()
        self.columns = {
            "Timestamp": "TIMESTAMP_NS", 
            "Timestamp_BS": "TIMESTAMP_NS",    
            "max_ser": "FLOAT", 
            "avg_ser": "FLOAT", 
            "fail_rate": "FLOAT"
        }
        self.table_name = "LTE_PHY_PDCCH_Decoding_Result"
        self.type_id = ["LTE_PHY_PDCCH_Decoding_Result"]
        
    def parse_to_db(self, msg, tree, db):
        timestamp = tree.find("pair[@key='device_timestamp']")
        if timestamp != None:
            timestamp = timestamp.text
        else:
            timestamp = tree.find("pair[@key='timestamp']").text
        bs_timestamp = tree.find("pair[@key='timestamp']").text
        records = tree.find("pair[@key='Hypothesis']/list")
        l = len(records.findall('item'))
        max_ser = 0
        total_ser = 0
        avg_ser = 0
        fail_rate = 0
        total_fail = 0
        for i in records.findall('item'):
            ser = float(i.find("dict/pair[@key='Symbol Error Rate']").text)
            max_ser = max(max_ser, ser)
            total_ser += ser
            if (i.find("dict/pair[@key='Prune Status']").text.find('SUCCESS') == -1):
                total_fail += 1
        fail_rate = total_fail / l
        avg_ser = total_ser / l
        db.sql(f"""
        INSERT INTO {self.table_name} VALUES (
            '{timestamp}', '{bs_timestamp}', {max_ser}, {avg_ser}, {fail_rate}
        );
        """)
        return True
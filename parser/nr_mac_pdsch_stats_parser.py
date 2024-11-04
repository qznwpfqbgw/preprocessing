from .parser import Parser
import xml.etree.ElementTree as ET
import datetime
import math
class NR_MAC_PDSCH_Stats_Parser(Parser):
    def __init__(self) -> None:
        super().__init__()
        self.columns = {
            "Timestamp": "TIMESTAMP_NS", 
            "Timestamp_BS": "TIMESTAMP_NS",  
            "BLER": "DOUBLE",
            "Num_Slots_Elapsed": "BIGINT",
            "Num_PDSCH_Decode": "BIGINT",
            "HARQ_Failure": "BIGINT",
        }
        self.table_name = "NR_MAC_PDSCH_Stats"
        self.type_id = ["5G_NR_MAC_PDSCH_Stats"]
        
    def parse_to_db(self, msg, tree, db):
        timestamp = tree.find("pair[@key='device_timestamp']")
        if timestamp != None:
            timestamp = timestamp.text
        else:
            timestamp = tree.find("pair[@key='timestamp']").text
        bs_timestamp = tree.find("pair[@key='timestamp']").text
        records = tree.find("pair[@key='Records']/list")
        bler = 0
        num_of_slot = 0
        num_of_decode = 0
        num_harq_fail = 0
        for i in records.findall('item'):
            tmp = i.find("dict/pair[@key='BLER (%)']").text
            try:
                bler = float(tmp)
                if math.isnan(bler):
                    bler = 0
            except Exception as e:
                bler = 0
            num_of_slot = int(i.find("dict/pair[@key='Num Slots Elapsed']").text)
            num_of_decode = int(i.find("dict/pair[@key='Num PDSCH Decode']").text)
            num_harq_fail = int(i.find("dict/pair[@key='HARQ Failure']").text)

        db.sql(f"""
        INSERT INTO {self.table_name} VALUES (
            '{timestamp}', '{bs_timestamp}', {bler}, {num_of_slot}, 
            {num_of_decode}, {num_harq_fail}
        );
        """)
        return True
from .parser import Parser
import xml.etree.ElementTree as ET
import datetime

class LTE_MAC_UL_Tx_Statistics_Parser(Parser):
    def __init__(self) -> None:
        super().__init__()
        self.columns = {
            "Timestamp": "TIMESTAMP_NS", 
            "Timestamp_BS": "TIMESTAMP_NS",  
            "grant_received": "BIGINT", 
            "grant_utilized": "BIGINT",
            "grant_utilization": "FLOAT",
            "num_of_padding_bsr": "BIGINT", 
            "num_of_regular_bsr": "BIGINT",
            "num_of_periodic_bsr": "BIGINT",
            "num_of_sample": "BIGINT"
        }
        self.table_name = "LTE_MAC_UL_Tx_Statistics"
        self.type_id = ["LTE_MAC_UL_Tx_Statistics"]
        
    def parse_to_db(self, msg, tree, db):
        timestamp = tree.find("pair[@key='device_timestamp']")
        if timestamp != None:
            timestamp = timestamp.text
        else:
            timestamp = tree.find("pair[@key='timestamp']").text
        bs_timestamp = tree.find("pair[@key='timestamp']").text
        grant_received = 0
        grant_utilized = 0
        grant_utilization = 0
        num_of_padding_bsr = 0
        num_of_regular_bsr = 0
        num_of_periodic_bsr = 0
        num_of_sample = 0

        sub_pkts = tree.find("pair[@key='Subpackets']/list")
        if sub_pkts != None:
            for i in sub_pkts.findall('item'):
                grant_received += int(i.find("dict/pair[@key='Sample']/dict/pair[@key='Grant received']").text)
                grant_utilized += int(i.find("dict/pair[@key='Sample']/dict/pair[@key='Grant utilized']").text)
                num_of_padding_bsr += int(i.find("dict/pair[@key='Sample']/dict/pair[@key='Number of padding BSR']").text)
                num_of_regular_bsr += int(i.find("dict/pair[@key='Sample']/dict/pair[@key='Number of regular BSR']").text)
                num_of_periodic_bsr += int(i.find("dict/pair[@key='Sample']/dict/pair[@key='Number of periodic BSR']").text)
                num_of_sample += int(i.find("dict/pair[@key='Sample']/dict/pair[@key='Number of samples']").text)
            
            if grant_received != 0:
                grant_utilization = round(
                    100.0 * grant_utilized / grant_received, 2)
        db.sql(f"""
        INSERT INTO {self.table_name} VALUES (
            '{timestamp}', '{bs_timestamp}', 
            {grant_received}, {grant_utilized}, {grant_utilization},
            {num_of_padding_bsr}, {num_of_regular_bsr}, {num_of_periodic_bsr}, 
            {num_of_sample}
        );
        """)
        return True
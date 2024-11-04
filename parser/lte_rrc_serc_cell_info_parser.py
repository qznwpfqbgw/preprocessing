from .parser import Parser
import xml.etree.ElementTree as ET
import datetime
import io
from bs4 import BeautifulSoup
class LTE_RRC_Serv_Cell_Info_Parser(Parser):
    def __init__(self) -> None:
        super().__init__()
        self.columns = {
            "Timestamp": "TIMESTAMP_NS", 
            "Timestamp_BS": "TIMESTAMP_NS",  
            "PCI": "BIGINT",
            "DL frequency": "BIGINT",
            "UL frequency": "BIGINT",
            "DL bandwidth": "VARCHAR",
            "UL bandwidth": "VARCHAR",
            "Cell Identity": "BIGINT",
            "TAC": "BIGINT",
            "Band ID": "BIGINT",
            "MCC": "BIGINT",
            "MNC": "BIGINT",
            "MNC digit": "BIGINT"
        }
        self.table_name = "LTE_RRC_Serv_Cell_Info"
        self.type_id = ["LTE_RRC_Serv_Cell_Info"]
        
    def parse_to_db(self, msg, tree, db):
        timestamp = tree.find("pair[@key='device_timestamp']")
        if timestamp != None:
            timestamp = timestamp.text
        else:
            timestamp = tree.find("pair[@key='timestamp']").text
        bs_timestamp = tree.find("pair[@key='timestamp']").text
        msg_io = io.StringIO(msg)
        l = msg_io.readline()
        data = ""
        if r"<dm_log_packet>" in l:
            
            soup = BeautifulSoup(l, 'html.parser')
            PCI = soup.find(key="Cell ID").get_text()
            DL_f = soup.find(key="Downlink frequency").get_text()
            UL_f = soup.find(key="Uplink frequency").get_text()
            DL_BW = soup.find(key="Downlink bandwidth").get_text()
            UL_BW = soup.find(key="Uplink bandwidth").get_text()
            Cell_identity = soup.find(key="Cell Identity").get_text()
            TAC = soup.find(key="TAC").get_text()
            Band_ID = soup.find(key="Band Indicator").get_text()
            MCC = soup.find(key="MCC").get_text()
            MNC_d = soup.find(key="MNC Digit").get_text()
            MNC = soup.find(key="MNC").get_text()
            data = [str(timestamp), str(bs_timestamp), PCI, DL_f, UL_f, DL_BW, UL_BW, Cell_identity, TAC, Band_ID, MCC, MNC, MNC_d]
            cnt = 0
            for i in self.columns.values():
                if i == "VARCHAR" or i == "TIMESTAMP_NS":
                    data[cnt] = "\'" + data[cnt] + "\'"
                cnt += 1
            data = ",".join(data)
            db.sql(f"""
                INSERT INTO {self.table_name} VALUES (
                    {data}
                );
            """)
        return True
from .parser import Parser
import xml.etree.ElementTree as ET
import datetime
import io
from bs4 import BeautifulSoup
class NR_ML1_Searcher_Measurement_Database_Update_Ext_Parser(Parser):
    def __init__(self) -> None:
        super().__init__()
        self.columns = {
            "Timestamp": "TIMESTAMP_NS", 
            "Timestamp_BS": "TIMESTAMP_NS",  
            "Raster ARFCN": "BIGINT",
            "Num Cells": "BIGINT",
            "Serving Cell Index": "BIGINT",
            "Serving Cell PCI": "BIGINT",
            "PCI0": "BIGINT",
            "RSRP0": "DOUBLE",
            "RSRQ0": "DOUBLE",
            "PCI1": "BIGINT",
            "RSRP1": "DOUBLE",
            "RSRQ1": "DOUBLE",
            "PCI2": "BIGINT",
            "RSRP2": "DOUBLE",
            "RSRQ2": "DOUBLE",
            "PCI3": "BIGINT",
            "RSRP3": "DOUBLE",
            "RSRQ3": "DOUBLE",
            "PCI4": "BIGINT",
            "RSRP4": "DOUBLE",
            "RSRQ4": "DOUBLE",
            "PCI5": "BIGINT",
            "RSRP5": "DOUBLE",
            "RSRQ5": "DOUBLE",
            "PCI6": "BIGINT",
            "RSRP6": "DOUBLE",
            "RSRQ6": "DOUBLE",
            "PCI7": "BIGINT",
            "RSRP7": "DOUBLE",
            "RSRQ7": "DOUBLE",
            "PCI8": "BIGINT",
            "RSRP8": "DOUBLE",
            "RSRQ8": "DOUBLE",
            "PCI9": "BIGINT",
            "RSRP9": "DOUBLE",
            "RSRQ9": "DOUBLE",
            "PCI10": "BIGINT",
            "RSRP10": "DOUBLE",
            "RSRQ10": "DOUBLE",
            "PCI11": "BIGINT",
            "RSRP11": "DOUBLE",
            "RSRQ11": "DOUBLE",
        }
        self.table_name = "NR_ML1_Searcher_Measurement_Database_Update_Ext"
        self.type_id = ["5G_NR_ML1_Searcher_Measurement_Database_Update_Ext"]
        
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
            arfcn = soup.find(key="Raster ARFCN").get_text()
            
            num_cells = soup.find(key="Num Cells").get_text()
            serving_cell_idex = soup.find(key="Serving Cell Index").get_text()
            serving_cell_pci = soup.find(key="Serving Cell PCI").get_text()
            pcis = [pci.get_text() for pci in soup.findAll(key="PCI")]
            rsrps = [rsrp.get_text() for rsrp in soup.findAll(key="Cell Quality Rsrp")]
            rsrqs = [rsrq.get_text() for rsrq in soup.findAll(key="Cell Quality Rsrq")]
            A = []
            for i in range(int(num_cells)):    
                A.append(pcis[i])
                A.append(rsrps[i])
                A.append(rsrqs[i])
            if int(num_cells) < 12:
                for i in range(12 - int(num_cells)):
                    A.append(0)
                    A.append(0)
                    A.append(0)
            A = [timestamp, bs_timestamp, arfcn, num_cells, serving_cell_idex, serving_cell_pci] + A
            A = [str(i) for i in A]
            cnt = 0
            for i in self.columns.values():
                if i == "VARCHAR" or i == "TIMESTAMP_NS":
                    A[cnt] = "\'" + A[cnt] + "\'"
                cnt += 1
            data = ",".join(A)
            db.sql(f"""
                INSERT INTO {self.table_name} VALUES (
                    {data}
                );
            """)
        return True
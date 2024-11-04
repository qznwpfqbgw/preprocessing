from .parser import Parser
import xml.etree.ElementTree as ET
import datetime
import io
from bs4 import BeautifulSoup
from itertools import chain
class LTE_PHY_Connected_Mode_Intra_Freq_Meas_Parser(Parser):
    def __init__(self) -> None:
        super().__init__()
        self.columns = {
            "Timestamp": "TIMESTAMP_NS", 
            "Timestamp_BS": "TIMESTAMP_NS",  
            "PCI": "BIGINT",
            "RSRP(dBm)": "DOUBLE",
            "RSRQ(dB)": "DOUBLE",
            "Serving Cell Index": "VARCHAR",
            "EARFCN": "BIGINT",
            "Number of Neighbor Cells": "BIGINT",
            "Number of Detected Cells": "BIGINT",
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
        self.table_name = "LTE_PHY_Connected_Mode_Intra_Freq_Meas"
        self.type_id = ["LTE_PHY_Connected_Mode_Intra_Freq_Meas"]
        
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
            try:
                pci = soup.find(key="Serving Physical Cell ID").get_text() ## This is current serving cell PCI.
            except:
                pci = "-"
            serving_cell = soup.find(key="Serving Cell Index").get_text()
            earfcn = soup.find(key="E-ARFCN").get_text()
            n_nei_c = soup.find(key="Number of Neighbor Cells").get_text()
            n_det_c = soup.find(key="Number of Detected Cells").get_text()
            rsrps = [rsrp.get_text() for rsrp in soup.findAll(key="RSRP(dBm)")]
            rsrqs = [rsrq.get_text() for rsrq in soup.findAll(key="RSRQ(dB)")]
            PCIs = [pci.get_text() for pci in soup.findAll(key="Physical Cell ID")]
            if int(n_det_c) != 0:
                PCIs = PCIs[:-int(n_det_c)]
            A = [[PCIs[i], rsrps[i+1], rsrqs[i+1]] for i in range(len(PCIs))] ## Information of neighbor cell
            A = list(chain.from_iterable(A))
            A = [timestamp, bs_timestamp, pci, rsrps[0], rsrqs[0], serving_cell, earfcn, n_nei_c, n_det_c] + A
            if len(A) < len(self.columns):
                for _ in range(len(self.columns) - len(A)):
                    A.append(0)
            A = [str(i) for i in A]
            cnt = 0
            for i in self.columns.values():
                A[cnt] = A[cnt].replace("'", "\\\"")
                if i == "VARCHAR" or i == 'TIMESTAMP_NS':
                    A[cnt] = "\'" + A[cnt] + "\'"
                cnt += 1
            data = ",".join(A)
            db.sql(f"""
                INSERT INTO {self.table_name} VALUES (
                    {data}
                );
            """)
        return True
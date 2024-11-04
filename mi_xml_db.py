# %%
import duckdb
import xml.etree.ElementTree as ET
import datetime
import math
from parser import *
from multiprocessing.pool import ThreadPool as Pool
from extension import *
class mi_xml_db:
    def __init__(self, fs, db, client = None, server = None):
        self.fs = open(fs)
        self.db_name = db
        self.db = duckdb.connect(db)
        self.db.sql('SET threads TO 1;')
        self.db.sql("SET memory_limit = '30GB';")
        self.cur_fn = None
        self.last_bytes = {}
        self.buffer = {}
        self.ctrl_pkt_sfn = {}
        self.outs = {}
        self.num = 0
        self.client_file_name = client
        self.server_file_name = server
        self.filter = None
        self.results = []
        self.parsers = [
            LTE_MAC_UL_Tx_Statistics_Parser(),
            LTE_MAC_UL_Buffer_Status_Internal_Parser(),
            LTE_PHY_PDCCH_Decoding_Result_Parser(),
            LTE_PHY_RLM_Report_Parser(),
            RRC_OTA_Packet_Parser(),
            NR_L2_UL_TB_Parser(),
            NR_MAC_PDSCH_Stats_Parser(),
            NR_ML1_Searcher_Measurement_Database_Update_Ext_Parser(),
            LTE_PHY_Connected_Mode_Intra_Freq_Meas_Parser(),
            LTE_RRC_Serv_Cell_Info_Parser()
        ]
        self.extension = [
            HO_Helper(self.db),
            MR_Helper(self.db)
        ]
        
    def dump_packet_into_db(self):
        if self.client_file_name != None:
            self.db.sql(f"CREATE TABLE CLIENT AS FROM read_csv('{self.client_file_name}', delim = '@' );")
        if self.server_file_name != None:
            self.db.sql(f"CREATE TABLE SERVER AS FROM read_csv('{self.server_file_name}', delim = '@' );")
    def read_next_msg(self):
        msg = ""
        begin = False
        while True:
            line = self.fs.readline()
            if not line:
                break

            if not begin:
                if '<dm_log_packet>' in line and '</dm_log_packet>' in line:
                    return line
                if '<dm_log_packet>' in line:
                    msg += line
                    begin = True
            else:
                msg += line
                if '</dm_log_packet>' in line:
                    return msg
        return None
    def set_filter(self, type_id):
        self.filter = type_id
    def msg_type_stat(self):
        stat = {}
        while True:
            msg = self.read_next_msg()
            if msg != None:
                tree = ET.fromstring(msg)
                type_id = tree.find("pair[@key='type_id']").text
                if type_id not in stat:
                    stat[type_id] = 0
                stat[type_id] += 1
            else:
                break
        print(stat)
        
        
    def parse_to_db(self):
        for i in self.parsers:
            cols_str = ',\n'.join([f'"{k}" {v}' for k, v in i.columns.items()])
            self.db.sql(f'CREATE TABLE IF NOT EXISTS {i.table_name} (\n{cols_str} \n);')
        
        while True:
            msg = self.read_next_msg()
            if msg != None:
                self.parse_msg(msg)
            else:
                break
        print("finish parse to db")

    def parse_msg(self, msg):
        tree = ET.fromstring(msg)

        type_id = tree.find("pair[@key='type_id']").text
        if self.filter != None and type_id not in self.filter:
            return
        for i in self.parsers:
            if type_id in i.type_id:
                try:
                    i.parse_to_db(msg, tree, self.db)
                except Exception:
                    import traceback
                    traceback.print_exc()
                    print("Error in ",self.fs.name)
                    raise Exception("Error in " + self.fs.name)
    
    def run_extension(self):
        try:
            for i in self.extension:
                i.run()
        except Exception:
                import traceback
                traceback.print_exc()
                print("Error run_extension() in ",self.db_name)
                raise Exception("Error in " + self.db_name)
# %%

if __name__ == "__main__":
    mi_xml = mi_xml_db('/home/wmnlab/G/database/2023-03-26/UDP_Bandlock_All_RM500Q/qc03/#02/middle/diag_log_qc03_2023-03-26_16-46-50.txt', 
                        "2024-05-01_6_test1.db")
    # mi_xml.filter = ['LTE_RRC_Serv_Cell_Info']
    #%%
    mi_xml.parse_to_db()
    # %%
    # mi_xml.db.close()
    df = mi_xml.db.sql('select * from RRC_OTA_Packet').fetchdf()
    from utils import *
    Ho = parse_mi_ho(df)
    MR = MeasureReport(df)
from .parser import Parser
import xml.etree.ElementTree as ET
import datetime

class LTE_MAC_UL_Buffer_Status_Internal_Parser(Parser):
    def __init__(self) -> None:
        super().__init__()
        self.columns = {
            "Timestamp": "TIMESTAMP_NS", 
            "Timestamp_BS": "TIMESTAMP_NS", 
            "cur_sub_fn": "BIGINT", 
            "cur_sys_fn": "BIGINT",
            "ld_id": "BIGINT", 
            "new_bytes": "BIGINT", 
            "ctrl_bytes": "BIGINT", 
            "total_bytes": "BIGINT", 
            "ctrl_pkt_delay": "BIGINT",
            "pkt_delay": "BIGINT", 
            "queue_length": "BIGINT"
        }
        self.table_name = "LTE_MAC_UL_Buffer_Status_Internal"
        self.type_id = "LTE_MAC_UL_Buffer_Status_Internal"
        
    def parse_to_db(self, msg, tree, db):
        return True
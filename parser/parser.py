class Parser:
    def __init__(self) -> None:
        self.type_id = []
        self.columns = {}
        self.table_name = ""
    
    def parse_to_db(self, msg, tree, db):
        pass
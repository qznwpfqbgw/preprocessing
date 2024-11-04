from .parser import Parser
import xml.etree.ElementTree as ET
import datetime
import re
import io
def get_text(l, NAME): ## Given l, return XXXX if NAME in l, else it will error, with format "NAME: XXXX".
    a = l.index('"' + NAME)
    k = len(NAME)+3
    b = l.index("\"", a+1)
    return l[a+k:b]

def passlines(num, f): ## Given num(int) and open file, read the file to next num line. 
    for i in range(num):
        l = f.readline()
    return l

def multi_output_write(type_code, c, type, l=None, sep='@'):
    if l is None:
        if type_code[c] == '0':
            type_code[c] = type
        else:
            type_code[c] = type_code[c] + sep +  type
    else:    
        if type_code[c] == '0':
            type_code[c] = get_text(l, type)
        else:
            type_code[c] = type_code[c] + sep +  get_text(l, type)
    

def find_next_str_and_write(type_code, f, L, c):
    l = f.readline()
    while l:
        for x in L:
            if x in l:
                multi_output_write(type_code, c, x, l)
                ans = get_text(l, x)
                return ans, l
        l = f.readline()
        
def get_meas_report_pairs(f, sep="&"): ## (MeasId & measObjectId & reportConfigId)
    l = f.readline()
    measId = get_text(l, "measId")
    l = f.readline()
    measObjectId = get_text(l, "measObjectId")
    l = f.readline()
    reportConfigId = get_text(l, "reportConfigId")
    return '('+measId+sep+measObjectId+sep+reportConfigId+')'

def get_event_paras(f, eventId, l):

    def lte_get_hys_and_ttt():
        l = passlines(4, f)
        hysteresis = get_text(l, "hysteresis")
        hysteresis = hysteresis.split(" ")[0]
        l = passlines(2, f)
        timeToTrigger = get_text(l, "timeToTrigger")
        timeToTrigger = timeToTrigger.split(" ")[0]
        return  hysteresis, timeToTrigger 
    
    def nr_get_hys_and_ttt():
        l = passlines(3, f)
        hysteresis = get_text(l, "hysteresis")
        hysteresis = hysteresis.split(" ")[0]
        l = passlines(2, f)
        timeToTrigger = get_text(l, "timeToTrigger")
        timeToTrigger = timeToTrigger.split(" ")[0]
        return  hysteresis, timeToTrigger 

    paras = {}
    if eventId == "eventA1 (0)" or eventId == "eventA2 (1)": ## A1 or A2
        if "\"lte-rrc.eventId\"" in l:
            l = passlines(4, f)
            if "\"lte-rrc.threshold_RSRQ\"" in l: # Use RSRQ for event A2
                threshold =  get_text(l, "threshold-RSRQ")   
            else: # Use RSRP for event A2
                threshold =  get_text(l, "threshold-RSRP")
            threshold = threshold.split(" ")[0]
            hysteresis, timeToTrigger = lte_get_hys_and_ttt()
            paras['thr'], paras['hys'], paras['ttt'] = threshold, hysteresis, timeToTrigger
        elif "\"nr-rrc.eventId\"" in l:
            l = passlines(4, f)

            threshold =  get_text(l, "rsrp")
            # Deal with some special case. 
            try:
                threshold = '[' + threshold.split(" ")[0] + ', ' + threshold.split(" ")[4] + ')'
            except:
                threshold = threshold.split(" ")[2]
            hysteresis, timeToTrigger = nr_get_hys_and_ttt()
            paras['thr'], paras['hys'], paras['ttt'] = threshold, hysteresis, timeToTrigger
    elif eventId == "eventA3 (2)": ## A3
        if "\"lte-rrc.eventId\"" in l:
            l = passlines(2, f)
            offset =  get_text(l, "a3-Offset")
            offset = offset.split(" ")[0]
            hysteresis, timeToTrigger = lte_get_hys_and_ttt()
            paras['off'], paras['hys'], paras['ttt'] = offset, hysteresis, timeToTrigger
        elif "\"nr-rrc.eventId\"" in l:
            l = passlines(4, f)
            offset = get_text(l, "rsrp")
            hysteresis, timeToTrigger = nr_get_hys_and_ttt()
            paras['off'], paras['hys'], paras['ttt'] = offset, hysteresis, timeToTrigger
    elif eventId == "eventA5 (4)": ## A5
        if "\"lte-rrc.eventId\"" in l:
            l = passlines(4, f)
            if "\"lte-rrc.threshold_RSRQ\"" in l: # Use RSRQ for event A5
                threshold1 =  get_text(l, "threshold-RSRQ")   
            else: # Use RSRP for event A5
                threshold1 =  get_text(l, "threshold-RSRP")
            threshold1 = threshold1.split(" ")[0]
            l = passlines(4, f)
            if "\"lte-rrc.threshold_RSRQ\"" in l: # Use RSRQ for event A5
                threshold2 =  get_text(l, "threshold-RSRQ")   
            else: # Use RSRP for event A5
                threshold2 =  get_text(l, "threshold-RSRP")
            threshold2 = threshold2.split(" ")[0]
            hysteresis, timeToTrigger = lte_get_hys_and_ttt()
            paras['thr1'], paras['thr2'], paras['hys'], paras['ttt'] = threshold1, threshold2, hysteresis, timeToTrigger
        elif "\"nr-rrc.eventId\"" in l:
            pass
    elif eventId == "eventA6-r10 (5)": ## A6
        if "\"lte-rrc.eventId\"" in l:
            l = passlines(2, f)
            offset =  get_text(l, "a6-Offset-r10")
            offset = offset.split(" ")[0]
            hysteresis, timeToTrigger = lte_get_hys_and_ttt()
            paras['off'], paras['hys'], paras['ttt'] = offset, hysteresis, timeToTrigger
        elif "\"nr-rrc.eventId\"" in l:
            pass
    elif eventId == "eventB1-NR-r15 (5)": ## interRAT B1
        if "\"lte-rrc.eventId\"" in l:
            l = passlines(4, f)
            offset =  get_text(l, "nr-RSRP-r15")
            offset = '[' + offset.split(" ")[0] + ', ' + offset.split(" ")[4] + ')'
            l = f.readline()
            hysteresis, timeToTrigger = lte_get_hys_and_ttt()
            paras['thr'], paras['hys'], paras['ttt'] = offset, hysteresis, timeToTrigger
        elif "\"nr-rrc.eventId\"" in l:
            pass
    else:
        pass    
    return str(paras).replace(',', '&')

class RRC_OTA_Packet_Parser(Parser):
    def __init__(self) -> None:
        self.type_id = ["LTE_RRC_OTA_Packet", "5G_NR_RRC_OTA_Packet"]
        self.columns = {
            "Timestamp": "TIMESTAMP_NS", 
            "Timestamp_BS": "TIMESTAMP_NS",  
            "type_id": "VARCHAR",
            "PCI": "BIGINT",
            "UL_DL": "VARCHAR",
            "Freq": "BIGINT",
            # Serving cell info
            # "DL frequency": "BIGINT",
            # "UL frequency": "BIGINT",
            # "DL bandwidth_MHZ": "BIGINT",
            # "UL bandwidth_MHZ": "BIGINT",
            # "Cell Identity": "BIGINT",
            # "TAC": "BIGINT",
            # "Band ID": "BIGINT",
            # "MCC": "BIGINT",
            # "MNC": "BIGINT",
            
            ## Measure report related
            "lte-measurementReport": "VARCHAR",
            "nr-measurementReport": "VARCHAR",
            "measId": "VARCHAR",
            "MeasResultEUTRA": "VARCHAR",
            "physCellId": "VARCHAR", ## LTE measured target PCI for MeasResultEUTRA 
            "MeasResultServFreqNR-r15": "VARCHAR", ## When lte and nr both HO, this will be emerged with MeasResultEUTRA.
            "pci-r15": "VARCHAR",
            "MeasResultNR": "VARCHAR",
            "physCellId.1": "VARCHAR", ## NR measured target PCI for MeasResultNR
            "measResultServingCell": "VARCHAR",
            "physCellId.2": "VARCHAR",
            "MeasResultCellNR-r15": "VARCHAR",
            "pci-r15(NR)": "VARCHAR",    ## NR measured target PCI for MeasResultCellNR-r15
            ###########################

            ## Configuration dissemination Related
            "lte-MeasObjectToAddMod": "VARCHAR",
            "nr-MeasObjectToAddMod": "VARCHAR",
            "measObjectId": "VARCHAR", 
            "measObject": "VARCHAR", ## measObjectEUTRA (0) OR measObjectNR-r15 (5)
            "carrierFreq": "VARCHAR", ## For EUTRA
            "carrierFreq-r15": "VARCHAR", ## For measObjectNR-r15
            "ssbFrequency": "VARCHAR", ## For measObjectNR

            "lte-ReportConfigToAddMod": "VARCHAR",
            "lte-reportConfigId": "VARCHAR",
            "triggerType": "VARCHAR", ## triggerType for 4G
            "lte-eventId": "VARCHAR",
            "lte-parameter": "VARCHAR",

            "nr-ReportConfigToAddMod": "VARCHAR",
            "nr-reportConfigId": "VARCHAR",
            "reportType": "VARCHAR", ## reportType for 5G  
            "nr-eventId": "VARCHAR",
            "nr-parameter": "VARCHAR",
            
            "lte-measIdToRemoveList": "VARCHAR",
            "lte-MeasIdToAddMod": "VARCHAR",## (MeasId & measObjectId & reportConfigId)
            "nr-MeasIdToAddMod": "VARCHAR",
            ###########################

            ## Basic reconfiguration
            "rrcConnectionReconfiguration": "VARCHAR",
            "rrcConnectionReconfigurationComplete": "VARCHAR",
            "RRCReconfiguration": "VARCHAR",
            "RRCReconfigurationComplete": "VARCHAR",
            ###########################

            ## LTE RLF related
            "rrcConnectionReestablishmentRequest": "VARCHAR",
            "physCellId.3": "VARCHAR", ## Target PCI for rrcConnectionReestablishmentRequest.
            "reestablishmentCause": "VARCHAR", ## ReestablishmentCause for rrcConnectionReestablishmentRequest.
            "rrcConnectionReestablishment": "VARCHAR",
            "rrcConnectionReestablishmentComplete": "VARCHAR",
            "rrcConnectionReestablishmentReject": "VARCHAR",
            ###########################

            ## Initial setup related
            "rrcConnectionRequest": "VARCHAR",
            "rrcConnectionSetup": "VARCHAR",
            "rrcConnectionSetupComplete": "VARCHAR",
            "securityModeCommand": "VARCHAR",
            "securityModeComplete": "VARCHAR",
            ###########################

            ## Cell reselection related
            "rrcConnectionRelease": "VARCHAR",
            "systemInformationBlockType1": "VARCHAR",
            ###########################

            ##  NSA mode SN setup and release 
            "nr-Config-r15: release (0)": "VARCHAR",
            "nr-Config-r15: setup (1)": "VARCHAR",
            "dualConnectivityPHR: release (0)": "VARCHAR",
            "dualConnectivityPHR: setup (1)": "VARCHAR",
            ###########################

            ## NSA mode SN RLF related
            "scgFailureInformationNR-r15": "VARCHAR",
            "failureType-r15": "VARCHAR", ##Failure cause of scgfailure .
            ###########################

            ## LTE and NR ho related
            "lte_targetPhysCellId": "VARCHAR", ## Handover target.
            "dl-CarrierFreq": "VARCHAR",
            "lte-rrc.t304": "VARCHAR",

            "nr_physCellId": "VARCHAR", ## NR measured target PCI
            "absoluteFrequencySSB": "VARCHAR",
            "nr-rrc.t304": "VARCHAR",
            ###########################
            

            ## SCell add and release 
            "sCellToReleaseList-r10": "VARCHAR",
            "SCellIndex-r10": "VARCHAR",
            "SCellToAddMod-r10": "VARCHAR",
            "SCellIndex-r10.1": "VARCHAR",
            "physCellId-r10": "VARCHAR",
            "dl-CarrierFreq-r10": "VARCHAR",
            ###########################

            ## ueCapabilityInformation
            "ueCapabilityInformation": "VARCHAR",
            "SupportedBandEUTRA": "VARCHAR",
            "bandEUTRA": "VARCHAR",
            ###########################
        }
        self.table_name = "RRC_OTA_Packet"
   
        self.type_list = [
 
        ## MeasurementReport Related 
        "\"lte-rrc.measurementReport_element\"",
        "\"nr-rrc.measurementReport_element\"",

        "measId",
        "\"MeasResultEUTRA\"",
        "physCellId",
        "\"MeasResultServFreqNR-r15\"",
        "pci-r15",
        "\"MeasResultNR\"",
        "physCellId",
        "\"measResultServingCell\"",
        "physCellId",
        "\"MeasResultCellNR-r15\"",
        "pci-r15",
        ###########################
        
        ## Configuration dissemination Related
        "\"lte-rrc.MeasObjectToAddMod_element\"",
        "\"nr-rrc.MeasObjectToAddMod_element\"",
        "measObjectId", 
        "measObject", 
        "carrierFreq", 
        "carrierFreq-r15",
        "ssbFrequency",

        "\"lte-rrc.ReportConfigToAddMod_element\"",
        "lte-reportConfigId",
        "triggerType", ## triggerType for 4G
        "lte-eventId",
        "lte-parameter",

        "\"nr-rrc.ReportConfigToAddMod_element\"",
        "nr-reportConfigId",    
        "reportType", ## reportType for 5G
        "nr-eventId",
        "nr-parameter",

        "\"lte-rrc.measIdToRemoveList\"",
        "\"lte-rrc.MeasIdToAddMod_element\"",
        "\"nr-rrc.MeasIdToAddMod_element\"",
        ###########################


        ## Basic reconfiguration
        "\"rrcConnectionReconfiguration\"",
        "\"rrcConnectionReconfigurationComplete\"",
        "\"RRCReconfiguration\"",
        "\"RRCReconfigurationComplete\"",
        ###########################

        ## LTE RLF related 
        "\"rrcConnectionReestablishmentRequest\"",
        "physCellId", 
        "reestablishmentCause",
        "\"rrcConnectionReestablishment\"",
        "\"rrcConnectionReestablishmentComplete\"",
        "\"rrcConnectionReestablishmentReject\"",
        ###########################

        ## Initial Setup related
        "\"lte-rrc.rrcConnectionRequest_element\"",
        "\"rrcConnectionSetup\"",
        "\"rrcConnectionSetupComplete\"",
        "\"securityModeCommand\"",
        "\"securityModeComplete\"",
        ###########################

        ## Cell reselection related
        "\"rrcConnectionRelease\"",
        "\"systemInformationBlockType1\"",
        ###########################

        ## NSA mode SN setup and release 
        "\"nr-Config-r15: release (0)\"",
        "\"nr-Config-r15: setup (1)\"",
        "\"dualConnectivityPHR: release (0)\"",
        "\"dualConnectivityPHR: setup (1)\"",
        ###########################

        ## NSA mode SN RLF related
        "\"scgFailureInformationNR-r15\"",
        "failureType-r15",
        ###########################

        ## LTE and NR ho related
        "\"lte-rrc.targetPhysCellId\"",
        "dl-CarrierFreq",
        "\"lte-rrc.t304\"",

        "\"nr-rrc.physCellId\"",
        "\"nr-rrc.absoluteFrequencySSB\"",
        "\"nr-rrc.t304\"",
        ###########################

        ## SCell add and release 
        "\"sCellToReleaseList-r10:",
        "SCellIndex-r10",
        "\"SCellToAddMod-r10\"",
        "sCellIndex-r10",
        "physCellId-r10",
        "dl-CarrierFreq-r10",
        ###########################

        ## ueCapabilityInformation
        "\"ueCapabilityInformation\"",
        "\"SupportedBandEUTRA\"",
        "bandEUTRA",
        ###########################
        ]
    def parse_to_db(self, msg, tree, db):
        timestamp = tree.find("pair[@key='device_timestamp']")
        if timestamp != None:
            timestamp = timestamp.text
        else:
            timestamp = tree.find("pair[@key='timestamp']").text
        bs_timestamp = tree.find("pair[@key='timestamp']").text
        type_id = tree.find("pair[@key='type_id']").text
        pci = int(tree.find("pair[@key='Physical Cell ID']").text)
        ul_dl = ""
        freq = int(tree.find("pair[@key='Freq']").text)

        
        msg_io = io.StringIO(msg)
        l = msg_io.readline()
        type_code = ["0"] * len(self.type_list)
        if r"<dm_log_packet>" in l:
            while l and r"</dm_log_packet>" not in l:
                if "UL-DCCH-Message" in l:
                    ul_dl = "UL"
                elif "DL-DCCH-Message" in l:
                    ul_dl = "DL"
                c = 0
                next = 0 
                for type in self.type_list:
                    if next != 0:
                        next -= 1
                        continue

                    if type in l and type ==  "\"lte-rrc.measurementReport_element\"":
                        type_code[c] = "1"
                        c+=2
                        l = passlines(10, msg_io)
                        type_code[c] = get_text(l, "measId")
                        next = 2
                    elif type in l and type ==  "\"nr-rrc.measurementReport_element\"" :
                        type_code[c] = "1"
                        c+=1
                        l = passlines(9, msg_io)
                        try :
                            type_code[c] = get_text(l, "measId")
                        except:
                            type_code[c] = "none"
                        next = 1
                    elif type in l and type == "\"MeasResultEUTRA\"":
                        type_code[c] = "1"
                        c += 1
                        l = passlines(2, msg_io)
                        multi_output_write(type_code, c, "physCellId", l)
                        next = 1
                    elif type in l and type == "\"MeasResultServFreqNR-r15\"":
                        type_code[c] = "1"
                        c += 1
                        l = passlines(8, msg_io)
                        type_code[c] = get_text(l, "pci-r15")
                        next = 1 
                    elif type in l and type == "\"MeasResultNR\"":
                        type_code[c] = "1"
                        c += 1
                        l = passlines(3, msg_io)
                        multi_output_write(type_code, c, "physCellId", l)
                        next = 1
                    elif type in l and type == "\"measResultServingCell\"":
                        type_code[c] = "1"
                        c += 1
                        l = passlines(3, msg_io)
                        multi_output_write(type_code, c, "physCellId", l)
                        next = 1
                    elif type in l and type == "\"MeasResultCellNR-r15\"":
                        type_code[c] = "1"
                        c += 1
                        l = passlines(3, msg_io)
                        multi_output_write(type_code, c, "pci-r15", l)
                        next = 1
                    elif type in l and (type == "\"lte-rrc.MeasObjectToAddMod_element\"" or type == "\"nr-rrc.MeasObjectToAddMod_element\""):
                        
                        if type == "\"lte-rrc.MeasObjectToAddMod_element\"":
                            type_code[c] = "1"
                            c += 2
                            l = msg_io.readline()
                            multi_output_write(type_code, c, "measObjectId", l)
                            c += 1 
                        elif type == "\"nr-rrc.MeasObjectToAddMod_element\"":
                            type_code[c] = "1"
                            c += 1
                            l = msg_io.readline()
                            multi_output_write(type_code, c, "measObjectId", l)
                            c += 1 

                        while l:
                            l = msg_io.readline()
                            if "\"lte-rrc.measObject\"" in l:
                                multi_output_write(type_code, c, "measObject", l)
                                c += 1
                                obj = get_text(l, "measObject")
                                l = passlines(9, msg_io)
                                if obj == 'measObjectEUTRA (0)':
                                    try:
                                        multi_output_write(type_code, c, "carrierFreq", l)
                                    except:
                                        pass
                                elif obj == 'measObjectNR-r15 (5)':
                                    c += 1
                                    multi_output_write(type_code, c, "carrierFreq-r15", l)
                                next = 5
                                break
                            elif "\"nr-rrc.measObject\"" in l:
                                multi_output_write(type_code, c, "measObject", l)
                                c += 1
                                obj = get_text(l, "measObject")
                                l = passlines(18, msg_io)
                                if obj == 'measObjectNR (0)':
                                    c += 2
                                    multi_output_write(type_code, c, "ssbFrequency", l)
                                next = 5
                                break
                    
                    elif type in l and type == "\"lte-rrc.ReportConfigToAddMod_element\"": 
                        type_code[c] = "1"
                        c += 1
                        l = msg_io.readline()
                        multi_output_write(type_code, c, "reportConfigId", l)
                        c += 1
                        triggerType, l = find_next_str_and_write(type_code, msg_io, ["triggerType", "reportType"], c)
                        c += 1
                        if triggerType == "event (0)":
                            eventId, l = find_next_str_and_write(type_code, msg_io, ["eventId"],c)
                            c += 1
                            paras = get_event_paras(msg_io, eventId, l)
                            multi_output_write(type_code, c, paras)
                        elif triggerType == "periodical (1)":
                            l = passlines(3, msg_io)
                            multi_output_write(type_code, c, "purpose", l)
                            c += 1
                            paras = r'{}'
                            multi_output_write(type_code, c, paras)
                        next = 4

                    elif type in l and type == "\"nr-rrc.ReportConfigToAddMod_element\"":
                        type_code[c] = "1"
                        c += 1
                        l = msg_io.readline()
                        multi_output_write(type_code, c, "reportConfigId", l)
                        c += 1
                        triggerType, l = find_next_str_and_write(type_code, msg_io, ["triggerType", "reportType"], c)
                        c += 1
                        if triggerType == "eventTriggered (1)":
                            eventId, l = find_next_str_and_write(type_code,msg_io,["eventId"],c)
                            c += 1
                            paras = get_event_paras(msg_io, eventId, l)
                            multi_output_write(type_code, c, paras)
                        next = 4

                    elif type in l and type == "\"lte-rrc.measIdToRemoveList\"":
                        n = ''.join(filter(str.isdigit, get_text(l, "measIdToRemoveList")))
                        n = int(n)
                        l = passlines(2, msg_io)
                        for i in range(n):
                            multi_output_write(type_code, c, get_text(l, "MeasId"))
                            l = passlines(3, msg_io)
                    elif type in l and (type == "\"lte-rrc.MeasIdToAddMod_element\"" or type == "\"nr-rrc.MeasIdToAddMod_element\""):
                        multi_output_write(type_code, c, get_meas_report_pairs(msg_io))
                    elif type in l and type == "\"rrcConnectionReestablishmentRequest\"":
                        type_code[c] = "1"
                        c += 1
                        l = passlines(6, msg_io)
                        type_code[c] = get_text(l, "physCellId")
                        c += 1 
                        l = passlines(4, msg_io)
                        type_code[c] = get_text(l, "reestablishmentCause")
                        next = 2
                    elif type in l and type == "\"scgFailureInformationNR-r15\"":
                        type_code[c] = "1"
                        c += 1
                        l = passlines(13, msg_io)
                        type_code[c] = get_text(l, "failureType-r15")
                        next = 1
                    elif type in l and type == "\"lte-rrc.targetPhysCellId\"":
                        type_code[c] = get_text(l, "targetPhysCellId")
                        c += 1
                        l = passlines(2, msg_io)
                        if "\"lte-rrc.t304\"" in l:
                            type_code[c] = 'intrafreq'
                            c += 1
                            type_code[c] = "1"
                            next = 2
                        else:
                            l = passlines(1, msg_io)
                            type_code[c] = get_text(l, "dl-CarrierFreq")
                            next = 1
                    elif type in l and type == "\"nr-rrc.physCellId\"": 
                        type_code[c] = get_text(l, "physCellId")
                    elif type in l and type == "\"nr-rrc.absoluteFrequencySSB\"":
                        type_code[c] = get_text(l, "absoluteFrequencySSB")
                    elif type in l and type == "\"sCellToReleaseList-r10:":
                        type_code[c] = get_text(l, "sCellToReleaseList-r10")
                        c += 1
                        num = int(re.sub( "[^0-9]", '', get_text(l, "sCellToReleaseList-r10")))
                        for i in range(num):
                            if i == 0:
                                l = passlines(2, msg_io)
                            else:
                                l = passlines(3, msg_io)
                            multi_output_write(type_code, c, "SCellIndex-r10", l)
                        # type_code[c] = get_text(l, "SCellIndex-r10")
                        next = 1
                    elif type in l and type == "\"SCellToAddMod-r10\"":
                        type_code[c] = "1"
                        c += 1
                        l = passlines(5, msg_io)
                        multi_output_write(type_code, c, "sCellIndex-r10", l)
                        # type_code[c] = get_text(l, "sCellIndex-r10")
                        c += 1
                        l = passlines(2, msg_io)
                        if "physCellId-r10" in l:
                            multi_output_write(type_code, c, "physCellId-r10", l)
                            # type_code[c] = get_text(l, "physCellId-r10")
                            c += 1
                            l = passlines(1, msg_io)
                            multi_output_write(type_code, c, "dl-CarrierFreq-r10", l)
                            # type_code[c] = get_text(l, "dl-CarrierFreq-r10")
                        else:
                            type_code[c] = 'nr or cqi report'
                            c += 1
                        next = 3
                    elif type in l and type == "\"SupportedBandEUTRA\"":
                        type_code[c] = "1"
                        c += 1
                        l = passlines(1, msg_io)
                        multi_output_write(type_code, c, "bandEUTRA", l)
                        next = 1
                    elif type in l and type not in ["physCellId", "measObjectId", "measObject", "reportConfigId", "measId","carrierFreq","bandEUTRA"]:
                        type_code[c] = "1"
                        
                    c += 1
                
                l = msg_io.readline()
        else:
            print("Error! Invalid data content.")
            return
        data = [str(timestamp), str(bs_timestamp), type_id, str(pci), ul_dl, str(freq)] + type_code
        cnt = 0
        for i in self.columns.values():
            data[cnt] = data[cnt].replace("'", "\\\"")
            if i == "VARCHAR" or i == "TIMESTAMP_NS":
                data[cnt] = "\'" + data[cnt] + "\'"
            cnt += 1
        # print(f"""
        # INSERT INTO {self.table_name} VALUES (
        #     {",".join(data)}
        # );
        # """)
        db.sql(f"""
        INSERT INTO {self.table_name} VALUES (
            {",".join(data)}
        );
        """)
        return True
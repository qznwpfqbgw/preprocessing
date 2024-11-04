#%%
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from enum import Enum
import traceback
import warnings

warnings.filterwarnings("ignore")
class PredictTarget(Enum):
    RLF = 0
    HO = 1
    
class LabelType(Enum):
    CLASSIFICATION = 0
    REGRESSION = 1
    
class EncodeType(Enum):
    ONE_HOT = 0
    LABEL = 1
    
def create_sampling_data(db_file, interval_seconds, max_predict_time = 10):

    db = duckdb.connect(db_file)
    time_df = db.sql("""
    select min(Timestamp) as start_time, max("Timestamp") as stop_time from RRC_OTA_Packet;      
    """).df()
    start_dt = time_df['start_time'][0].replace(microsecond=0) + timedelta(seconds=interval_seconds)
    end_dt = time_df['stop_time'][0].replace(microsecond=0)
    # 生成时间数组
    time_array = [start_dt + timedelta(seconds=i) for i in np.arange(0, (end_dt - start_dt).total_seconds() + interval_seconds, interval_seconds)]
    # 将时间数组放入 DataFrame 中
    df = pd.DataFrame(time_array, columns=['Time'])
    
    ho_sample_df = db.sql(f"""
    select d2.Time, type as ho_type, start as ho_start, "end" as ho_end, others as ho_others, trans from df as d2 left join (
    select d1.Time as asof_time, * from df as d1
    ASOF JOIN HO_Table
    on d1.Time >= HO_Table.start
    and epoch(d1.Time - HO_Table.start) <= {interval_seconds}
    ) on asof_time = d2.Time ORDER BY d2.Time
    """)
    mr_sample_df = db.sql(f"""
    select d2.Time, type as mr_type, mr_time, event as mr_event, others as mr_others from df as d2 
    left join (
    select d1.Time as asof_time, MR_Table.time as mr_time, * from df as d1
    ASOF JOIN MR_Table
    on d1.Time >= MR_Table.time
    and epoch(d1.Time - MR_Table.time) <= {interval_seconds}
    ) on asof_time = d2.Time ORDER BY d2.Time                    
    """)
    nr_phy_col = list(db.sql("SHOW NR_ML1_Searcher_Measurement_Database_Update_Ext").df()['column_name'])
    lte_phy_col = list(db.sql("SHOW LTE_PHY_Connected_Mode_Intra_Freq_Meas").df()['column_name'])

    nr_phy_col_str = ', '.join([f"\"{col}\" as nr_phy_{col.replace(' ','_').replace('(','_').replace(')','_')}" for col in nr_phy_col])
    lte_phy_col_str = ', '.join([f"\"{col}\" as lte_phy_{col.replace(' ','_').replace('(','_').replace(')','_')}" for col in lte_phy_col])
    table_name = "NR_ML1_Searcher_Measurement_Database_Update_Ext"
    time_col_name = "Timestamp"
    col_str = nr_phy_col_str
    nr_phy_df = db.sql(f"""
    select d2.Time, {col_str} from df as d2 
    left join (
    select d1.Time as asof_time, {table_name}.{time_col_name} as match_time, * from df as d1
    ASOF JOIN {table_name}
    on d1.Time >= {table_name}.{time_col_name}
    and epoch(d1.Time - {table_name}.{time_col_name}) <= {interval_seconds}
    ) on asof_time = d2.Time ORDER BY d2.Time                    
    """)

    table_name = "LTE_PHY_Connected_Mode_Intra_Freq_Meas"
    time_col_name = "Timestamp"
    col_str = lte_phy_col_str

    lte_phy_df = db.sql(f"""
    select d2.Time, {col_str} from df as d2 
    left join (
    select d1.Time as asof_time, {table_name}.{time_col_name} as match_time, * from df as d1
    ASOF JOIN {table_name}
    on d1.Time >= {table_name}.{time_col_name}
    and epoch(d1.Time - {table_name}.{time_col_name}) <= {interval_seconds}
    ) on asof_time = d2.Time ORDER BY d2.Time                    
    """)
    
    
    ho_type_all_possible_type = ['LTE_HO', 'MN_HO', 'MN_HO_to_eNB', 'SN_HO']
    ho_condition_str = 'or'.join([f" type = '{ho_t}' " for ho_t in ho_type_all_possible_type])
    rlf_type_all_possible_type = ['RLF_II', 'RLF_III', 'SCG_RLF']
    rlf_condition_str = 'or'.join([f" type = '{rlf_t}' " for rlf_t in rlf_type_all_possible_type])

    time_to_ho_df = db.sql(f"""
    select d2.Time, COALESCE(time_to_ho, {max_predict_time}) as time_to_ho from df as d2 left join (
    select d1.Time as asof_time, epoch(t1.start - d1.Time) as time_to_ho from df as d1
    ASOF JOIN (
        select * from HO_Table where {ho_condition_str} 
    ) t1
    on d1.Time <= t1.start 
    and epoch(t1.start - d1.Time) <= {max_predict_time}
    ) on asof_time = d2.Time ORDER BY d2.Time
    """)
    
    time_to_rlf_df = db.sql(f"""
    select d2.Time, COALESCE(time_to_rlf, {max_predict_time}) as time_to_rlf from df as d2 left join (
    select d1.Time as asof_time, epoch(t1.start - d1.Time) as time_to_rlf from df as d1
    ASOF JOIN (
        select * from HO_Table where {rlf_condition_str} 
    ) t1
    on d1.Time <= t1.start 
    and epoch(t1.start - d1.Time) <= {max_predict_time}
    ) on asof_time = d2.Time ORDER BY d2.Time
    """)
    
    
    sampling_df = db.sql("""
    select * from ho_sample_df
    left join mr_sample_df
    on ho_sample_df.Time = mr_sample_df.Time 
    left join nr_phy_df 
    on ho_sample_df.Time = nr_phy_df.Time
    left join lte_phy_df 
    on ho_sample_df.Time = lte_phy_df.Time
    left join time_to_rlf_df 
    on ho_sample_df.Time = time_to_rlf_df.Time
    left join time_to_ho_df 
    on ho_sample_df.Time = time_to_ho_df.Time
    ORDER BY ho_sample_df.Time 
    """).df()
    db.close()
    return sampling_df

def phy_feature_select(sampling_df):
    
    nr_phy_rsrq_cols = [f'nr_phy_RSRQ{i}' for i in range(12)]
    nr_phy_rsrp_cols = [f'nr_phy_RSRP{i}' for i in range(12)]
    lte_phy_rsrq_cols = [f'lte_phy_RSRQ{i}' for i in range(1, 12)]
    lte_phy_rsrp_cols = [f'lte_phy_RSRP{i}' for i in range(1, 12)]
    
    sampling_df[nr_phy_rsrq_cols + nr_phy_rsrp_cols + lte_phy_rsrq_cols + lte_phy_rsrp_cols].fillna(-200, inplace=True)

    # Best RSRQ and RSRP values for NR and LTE (find maximum per row)
    nr_best_rsrq = sampling_df[nr_phy_rsrq_cols].max(axis=1)
    nr_best_rsrp = sampling_df[nr_phy_rsrp_cols].max(axis=1)
    lte_best_rsrq = sampling_df[lte_phy_rsrq_cols].max(axis=1)
    lte_best_rsrp = sampling_df[lte_phy_rsrp_cols].max(axis=1)
    
    # Handling the current PHY based on the Serving Cell Index
    current_nr_mask = (~sampling_df['nr_phy_Serving_Cell_Index'].isna()) & \
                    (sampling_df['nr_phy_Serving_Cell_Index'] != 255)

    # Extract NR values for serving cell
    sampling_df.loc[current_nr_mask, 'current_nr_RSRQ'] = sampling_df.apply(
        lambda row: row[f"nr_phy_RSRQ{int(row['nr_phy_Serving_Cell_Index'])}"] 
        if not np.isnan(row['nr_phy_Serving_Cell_Index']) and int(row['nr_phy_Serving_Cell_Index']) != 255
        else -200, axis=1
    )
    sampling_df.loc[current_nr_mask, 'current_nr_RSRP'] = sampling_df.apply(
        lambda row: row[f"nr_phy_RSRP{int(row['nr_phy_Serving_Cell_Index'])}"] 
        if not np.isnan(row['nr_phy_Serving_Cell_Index']) and int(row['nr_phy_Serving_Cell_Index']) != 255
        else -200, axis=1
    )
   
    sampling_df.loc[sampling_df['lte_phy_Serving_Cell_Index'] == 'PCell', ['current_lte_RSRQ', 'current_lte_RSRP']] = sampling_df[['lte_phy_RSRQ_dB_', 'lte_phy_RSRP_dBm_']].values[sampling_df['lte_phy_Serving_Cell_Index'] == 'PCell']
    sampling_df.loc[sampling_df['lte_phy_Serving_Cell_Index'] == '1_SCell', ['scell1_RSRQ', 'scell1_RSRP']] = sampling_df[['lte_phy_RSRQ_dB_', 'lte_phy_RSRP_dBm_']].values[sampling_df['lte_phy_Serving_Cell_Index'] == '1_SCell']
    sampling_df.loc[sampling_df['lte_phy_Serving_Cell_Index'] == '2_SCell', ['scell2_RSRQ', 'scell2_RSRP']] = sampling_df[['lte_phy_RSRQ_dB_', 'lte_phy_RSRP_dBm_']].values[sampling_df['lte_phy_Serving_Cell_Index'] == '2_SCell']
    sampling_df.loc[sampling_df['lte_phy_Serving_Cell_Index'] == '(MI)Unknown', ['scell3_RSRQ', 'scell3_RSRP']] = sampling_df[['lte_phy_RSRQ_dB_', 'lte_phy_RSRP_dBm_']].values[sampling_df['lte_phy_Serving_Cell_Index'] == '(MI)Unknown']

    result_df = pd.DataFrame({
        'nr_best_rsrq': nr_best_rsrq,
        'nr_best_rsrp': nr_best_rsrp,
        'lte_best_rsrq': lte_best_rsrq,
        'lte_best_rsrp': lte_best_rsrp,
        'current_nr_rsrq': sampling_df['current_nr_RSRQ'],
        'current_nr_rsrp': sampling_df['current_nr_RSRP'],
        'current_lte_rsrq': sampling_df['current_lte_RSRQ'],
        'current_lte_rsrp': sampling_df['current_lte_RSRP'],
        'scell1_lte_rsrq': sampling_df['scell1_RSRQ'],
        'scell1_lte_rsrp': sampling_df['scell1_RSRP'],
        'scell2_lte_rsrq': sampling_df['scell2_RSRQ'],
        'scell2_lte_rsrp': sampling_df['scell2_RSRP'],
        'scell3_lte_rsrq': sampling_df['scell3_RSRQ'],
        'scell3_lte_rsrp': sampling_df['scell3_RSRP'],
    })
    return result_df

def one_hot_helper(df, col_name, all_possible_data):
    all_possible_ho_type = all_possible_data
    df_onehot = pd.get_dummies(df[col_name],  dtype='int', columns=[col_name], dummy_na=False).reindex(columns=all_possible_ho_type, fill_value=0)
    return df_onehot

def create_train_data(db_file, interval_seconds, num_of_x_seq, num_of_y_seq, targets = [PredictTarget.RLF], y_type = LabelType.CLASSIFICATION, encoding = EncodeType.LABEL):
    Xt_list, Yt_list = None, None
    try:
        sampling_df = create_sampling_data(db_file, interval_seconds)

        col_name = "ho_type"
        ho_type_all_possible_data = ['LTE_HO', 'MN_HO', 'MN_HO_to_eNB', 'SN_setup', 'SN_Rel', 'SN_HO', 'Conn_Req' ,'RLF_II', 'RLF_III', 'SCG_RLF']
        ho_type_df = one_hot_helper(sampling_df, col_name, ho_type_all_possible_data)
        ho_type_df['LTE_HO'] = (ho_type_df['LTE_HO'] | ho_type_df['MN_HO_to_eNB']).astype(int)

        del ho_type_df['MN_HO_to_eNB']
        ho_type_all_possible_data = ['LTE_HO', 'MN_HO', 'SN_setup', 'SN_Rel', 'SN_HO', 'Conn_Req' ,'RLF_II', 'RLF_III', 'SCG_RLF']

        col_name = "mr_type"
        mr_type_all_possible_data = ['eventA1','eventA2','E-UTRAN-eventA3', 'eventA5', 'eventA6','NR-eventA3', 'eventB1-NR-r15','reportCGI', 'reportStrongestCells', 'others']
        mr_type_df = one_hot_helper(sampling_df, col_name, mr_type_all_possible_data)
        
        
        phy_df = pd.DataFrame()
        # 定義列的範圍
        nr_indices = range(12)
        lte_indices = range(1, 12)

        # 創建相應的列名
        nr_phy_cols = [f'nr_phy_RSRQ{i}' for i in nr_indices] + [f'nr_phy_RSRP{i}' for i in nr_indices]
        lte_phy_cols = [f'lte_phy_RSRQ{i}' for i in lte_indices] + [f'lte_phy_RSRP{i}' for i in lte_indices]

        # 最終合併所有列
        phy_helper_cols = (
            nr_phy_cols +
            lte_phy_cols +
            [
                'nr_phy_Serving_Cell_Index',
                'lte_phy_Serving_Cell_Index',
                'lte_phy_RSRQ_dB_',
                'lte_phy_RSRP_dBm_'
            ]
        )
        checking_feature = ['lte_phy_RSRP_dBm_']
        
        # 使用 ffill 並限制最多填充未來 3 秒
        sampling_df[phy_helper_cols] = sampling_df[phy_helper_cols].fillna(method='ffill', limit=int(3/interval_seconds))
        sampling_df[phy_helper_cols] = sampling_df[phy_helper_cols].fillna(method='bfill', limit=int(3/interval_seconds))
        phy_df = phy_feature_select(sampling_df[phy_helper_cols])
        phy_df.fillna(-200, inplace = True)
        
        features = ['lte_phy_EARFCN', 'lte_phy_Number_of_Neighbor_Cells', 'nr_phy_Num_Cells']
        others_df = sampling_df[features]
        others_df['lte_phy_EARFCN'] = others_df['lte_phy_EARFCN'].fillna(0)
        others_df['lte_phy_Number_of_Neighbor_Cells'] = others_df['lte_phy_Number_of_Neighbor_Cells'].fillna(0)
        others_df['nr_phy_Num_Cells'] = others_df['nr_phy_Num_Cells'].fillna(0)
        
        x_all_data = pd.concat([ho_type_df, mr_type_df, others_df, phy_df, sampling_df[checking_feature]], axis=1)
        y_all_data = pd.DataFrame()

        if y_type == LabelType.CLASSIFICATION:
            y_all_data['HO'] = (x_all_data['LTE_HO'] | x_all_data['MN_HO'] | x_all_data['SN_HO']).astype(int)
            y_all_data['RLF'] = (x_all_data['RLF_II'] | x_all_data['RLF_III'] | x_all_data['SCG_RLF']).astype(int)
            if PredictTarget.RLF in targets and PredictTarget.HO in targets:
                if encoding == EncodeType.LABEL:
                    y_all_data['OUT'] = 2 * y_all_data['HO'] + y_all_data['RLF']
                else:
                    y_all_data['OUT'] = y_all_data['RLF']
                    y_all_data['OUT2'] = y_all_data['HO']
                    y_all_data['OUT3'] = ((y_all_data['HO'] == 0) & (y_all_data['RLF'] == 0)).astype(int)
            elif PredictTarget.RLF in targets:
                if encoding == EncodeType.LABEL:
                    y_all_data['OUT'] = y_all_data['RLF']
                else:
                    y_all_data['OUT'] = y_all_data['RLF']
                    y_all_data['OUT2'] = (y_all_data['RLF'] == 0).astype(int)
                    
            elif PredictTarget.HO in targets:
                if encoding == EncodeType.LABEL:
                    y_all_data['OUT'] = y_all_data['HO']
                else:
                    y_all_data['OUT'] = y_all_data['HO']
                    y_all_data['OUT2'] = (y_all_data['HO'] == 0).astype(int)
                    
            del y_all_data['RLF']
            del y_all_data['HO']
        else:
            if PredictTarget.RLF in targets and PredictTarget.HO in targets:
                # y_all_data['OUT'] = sampling_df[]
                pass
            elif PredictTarget.RLF in targets:
                y_all_data['OUT'] = sampling_df['time_to_rlf']
            elif PredictTarget.HO in targets:
                y_all_data['OUT'] = sampling_df['time_to_ho']            

        Xt_list = []
        Yt_list = []
        for i in range(num_of_x_seq):
            X_t = x_all_data.shift(periods=-i)
            X_t = X_t.to_numpy()
            Xt_list.append(X_t)
        Xt_list = np.stack(Xt_list, axis=0)
        Xt_list = np.transpose(Xt_list, (1,0,2))
        Xt_list = Xt_list[:-(num_of_x_seq + num_of_y_seq -1), :, :]
        
        if y_type == LabelType.CLASSIFICATION:
            for i in range(num_of_x_seq, num_of_x_seq + num_of_y_seq):
                Y_t = y_all_data.shift(periods=-i)
                Y_t = Y_t.to_numpy()
                Yt_list.append(Y_t)
                
            Yt_list = np.stack(Yt_list, axis=0)
            Yt_list = np.transpose(Yt_list, (1,0,2))
            Yt_list = Yt_list[:-(num_of_x_seq + num_of_y_seq -1), :, :]
        else:
            Y_t = y_all_data.shift(periods=-(num_of_x_seq-1))
            Y_t = Y_t.to_numpy()
            Yt_list.append(Y_t)
            Yt_list = np.stack(Yt_list, axis=0)
            Yt_list = np.transpose(Yt_list, (1,0,2))
            Yt_list = Yt_list[:-(num_of_x_seq + num_of_y_seq -1), :, :]  
        

        is_all_nan = np.all(np.isnan(Xt_list[:, :, Xt_list.shape[2] - 1]), axis=1)
        indices_to_remove = np.where(is_all_nan)[0]
        Xt_list = np.delete(Xt_list, indices_to_remove, axis=0)
        
        # delete check data
        Xt_list = np.delete(Xt_list, [Xt_list.shape[2] - 1], axis=2)
        
        Yt_list = np.delete(Yt_list, indices_to_remove, axis=0)

        assert(not np.isnan(Xt_list).any())
        assert(not np.isnan(Yt_list).any())
        
    except Exception:
    
        traceback.print_exc()
        print(f"ERROR in {db_file}")
    return Xt_list, Yt_list
# %%

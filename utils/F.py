from tqdm.notebook import tqdm
import pandas as pd
import numpy as np
import warnings

from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, average_precision_score

def ts_array_create(data_list, time_seq_len, pred_time, features, ffill_cols=[],two_hot_cols=[],merged_cols=[]):

    X_all = []
    Y_all_cls = []
    Y_all_fst = []
    files_record = []
    
    def vecot_to_num(v):
        num = 0.0
        for i, t in enumerate(v):
            if t != 0:
                num = i+t
                break
        return num

    def replace_zero_with_one(value):
        if value == 0:
            return 0
        else:
            return 1

    count = 0
    for file in tqdm(data_list):

        df = pd.read_csv(file)

        # Hard to change to a feature, delete it now.
        del df['Timestamp'], df['PCI'], df['EARFCN'], df['NR-PCI']

        # Two hot column
        for col in two_hot_cols:
            df[col] = df[col].apply(replace_zero_with_one)
            
        for col in ffill_cols:
            df[col] = df[col].mask(df[col] == 0, pd.NA).ffill()
        # df[ffill_cols] = df[ffill_cols].replace(0, pd.NA)
        df[ffill_cols] = df[ffill_cols].ffill()
        
        for col in ffill_cols:
            if not pd.notna(df[col].iloc[0]):
                df = df[df[col].notna()]
        df.reset_index(drop=True, inplace=True)
        
        X = df[features]
        # Merged columns
        for cols in merged_cols:
            new_column = X[cols[:-1]].max(axis=1)
            col_num = X.columns.get_loc(cols[0])
            X = X.drop(cols[:-1], axis=1)
            X.insert(col_num, cols[-1], new_column)
        
        target = ['RLF_II', 'RLF_III']
        Y = df[target].copy()
        Y['RLF'] = Y.apply(lambda row: max(row['RLF_II'], row['RLF_III']), axis=1)
        Y.drop(columns=target, inplace=True)

        Xt_list = []
        Yt_list = []

        for i in range(time_seq_len):
            X_t = X.shift(periods=-i)
            X_t = X_t.to_numpy()
            Xt_list.append(X_t)

        Xt_list = np.stack(Xt_list, axis=0)
        Xt_list = np.transpose(Xt_list, (1,0,2))
        Xt_list = Xt_list[:-(time_seq_len + pred_time -1), :, :]

        for i in range(time_seq_len, time_seq_len+pred_time):
            Y_t = Y.shift(periods=-i)
            Y_t = Y_t.to_numpy()
            Yt_list.append(Y_t)
        
        Yt_list = np.stack(Yt_list, axis=0)
        Yt_list = np.transpose(Yt_list, (1,0,2))
        Yt_list = Yt_list[:-(time_seq_len + pred_time -1), :, :]
        # Yt_list = np.squeeze(Yt_list)
        # print(Xt_list.shape)
        # print(Yt_list.shape)
        if pred_time == 1: 
            Yt_cls = np.where(Yt_list != 0, 1, 0) 
            Yt_fst = Yt_list
        else: 
            Yt_cls = np.where((Yt_list != 0), 1, 0)
            Yt_fst = np.apply_along_axis(vecot_to_num, axis=1, arr=Yt_list)

        X_all.append(Xt_list)
        Y_all_cls.append(Yt_cls)
        Y_all_fst.append(Yt_fst)
        files_record.append((file, (count, count +len(Yt_cls))))
        count += len(Yt_cls)
        
    X_all = np.concatenate(X_all, axis=0)
    Y_all_cls = np.concatenate(Y_all_cls, axis=0)
    Y_all_fst = np.concatenate(Y_all_fst, axis=0)
    # print(X_all.shape)
    # print(Y_all_cls.shape)
    
    return X_all, Y_all_cls, Y_all_fst, files_record

def ts_array_create_v2(data_list, time_seq_len, pred_time, data_res, features, ffill_cols=[],two_hot_cols=[],merged_cols=[]):
    
    warnings.simplefilter(action='ignore', category=FutureWarning)
    
    X_all = []
    Y_all_cls = []
    Y_all_fst = []
    files_record = []
        
    def vecot_to_num(v):
        num = 0.0
        for i, t in enumerate(v):
            if t != 0:
                num = i+t
                break
        return num

    def replace_zero_with_one(value):
        if value == 0:
            return 0
        else:
            return 1

    count = 0
    for file in tqdm(data_list):
        df = pd.read_csv(file)
        del df['Timestamp'], df['PCI'], df['EARFCN'], df['NR-PCI']

        target = ['RLF_II', 'RLF_III']
        Y = df[target].copy()
        Y['RLF'] = Y.apply(lambda row: max(row['RLF_II'], row['RLF_III']), axis=1)
        Y.drop(columns=target, inplace=True)
        df['label'] = Y['RLF']

        # Two hot column
        for col in two_hot_cols:
            df[col] = df[col].apply(replace_zero_with_one)

        df[ffill_cols] = df[ffill_cols].applymap(lambda x: pd.NA if x == 0 else x)
        df[ffill_cols] = df[ffill_cols].ffill()
        for col in ffill_cols:
            if not pd.notna(df[col].iloc[0]):
                df = df[df[col].notna()]
        df.reset_index(drop=True, inplace=True)

        X = df[features]
        # Merged columns
        for cols in merged_cols:
            new_column = X[cols[:-1]].max(axis=1)
            col_num = X.columns.get_loc(cols[0])
            X = X.drop(cols[:-1], axis=1)
            X.insert(col_num, cols[-1], new_column)

        Y = df['label']

        multiple = int(1/data_res)
        All_Xt = []
        All_Yt_cls = []
        All_Yt_fst = []
        for start in range(multiple):
            Xt_list = []
            Yt_list = []
            X_ = pd.DataFrame(columns=X.columns)
            Y_ = pd.Series(dtype='float64')
            assert len(X) == len(Y)
            for i in range(start, len(X), multiple):
                if i+10 > len(X):
                    info = X.iloc[i: len(X)]
                    y_info = Y.iloc[i: len(X)]
                else:
                    info = X.iloc[i: i+10]
                    y_info = Y.iloc[i: i+10]
                # deal with feature
                d = {}
                for col in X.columns:
                    v = 0
                    for val in info[col]:
                        if val != 0: v = val # Last Informated
                    d[col] = v
                if X_.empty: 
                    X_ = pd.DataFrame( {k:[v] for k, v in d.items()} )
                else: 
                    new_row = pd.DataFrame([d])
                    X_ = pd.concat([X_, new_row], ignore_index=True)
                
                # deal with label
                v = 0; count = 0
                for val in y_info:
                    if val != 0:
                        count += val
                        v = count
                        break
                    else:
                        count += data_res
                if Y_.empty: 
                    Y_ = pd.Series([v])
                else:
                    new_data  =  pd.Series([v])
                    Y_ = pd.concat([Y_, new_data], ignore_index=True)
            # create feature time series 
            for j in range(time_seq_len):
                X_t = X_.shift(periods=-j)
                X_t = X_t.to_numpy()
                Xt_list.append(X_t)

            Xt_list = np.stack(Xt_list, axis=0)
            Xt_list = np.transpose(Xt_list, (1,0,2))
            Xt_list = Xt_list[:-(time_seq_len + pred_time -1), :, :]
            # create label value
            for j in range(time_seq_len, time_seq_len+pred_time):
                Y_t = Y_.shift(periods=-j)
                Y_t = Y_t.to_numpy()
                Yt_list.append(Y_t)
            
            Yt_list = np.stack(Yt_list, axis=0)
            Yt_list = np.transpose(Yt_list, (1,0))
            Yt_list = Yt_list[:-(time_seq_len + pred_time -1), :]

            if pred_time == 1: 
                Yt_cls = np.where(Yt_list != 0, 1, 0) 
                Yt_fst = Yt_list
            else: 
                Yt_cls = np.where((Yt_list != 0).any(axis=1), 1, 0)
                Yt_fst = np.apply_along_axis(vecot_to_num, axis=1, arr=Yt_list)
            
            All_Xt.append(Xt_list)
            All_Yt_cls.append(Yt_cls) 
            All_Yt_fst.append(Yt_fst)
            
        All_Xt = interleave_3d_arrays(All_Xt)
        All_Yt_cls = interleave_1d_arrays(All_Yt_cls)
        All_Yt_fst = interleave_1d_arrays(All_Yt_fst)
        
        X_all.append(All_Xt)
        Y_all_cls.append(All_Yt_cls)
        Y_all_fst.append(All_Yt_fst)
        files_record.append((file, (count, count + len(All_Yt_cls))))
        count += len(All_Yt_cls)
    X_all = np.concatenate(X_all, axis=0)
    Y_all_cls = np.concatenate(Y_all_cls, axis=0)
    Y_all_fst = np.concatenate(Y_all_fst, axis=0)

    return X_all, Y_all_cls, Y_all_fst, files_record

def interleave_1d_arrays(arrays):
    max_length = max(len(array) for array in arrays)
    
    filled_arrays = [
        np.pad(array, (0, max_length - len(array)), mode='constant', constant_values=0)
        for array in arrays
    ]
    
    interleaved_array = np.empty(len(arrays) * max_length, dtype=filled_arrays[0].dtype)
    
    for i in range(len(filled_arrays)):
        interleaved_array[i::len(filled_arrays)] = filled_arrays[i]
    
    total_length = sum(len(array) for array in arrays)
    interleaved_array = interleaved_array[:total_length]
    
    return interleaved_array

def interleave_3d_arrays(arrays):
    max_length = max(array.shape[0] for array in arrays)
    
    filled_arrays = [
        np.pad(array, ((0, max_length - array.shape[0]), (0, 0), (0, 0)), mode='constant', constant_values=0)
        for array in arrays
    ]

    interleaved_array = np.empty((len(arrays) * max_length, arrays[0].shape[1], arrays[0].shape[2]))

    for i in range(len(filled_arrays)):
        interleaved_array[i::len(filled_arrays)] = filled_arrays[i]
    
    total_length = sum(array.shape[0] for array in arrays)
    interleaved_array = interleaved_array[:total_length]
    
    return interleaved_array

# performance
def performance(model, dtest, y_test):
    
    y_pred_proba = model.predict(dtest)
    y_pred = (y_pred_proba > 0.5).astype(int)

    ACC = accuracy_score(y_test, y_pred)
    AUC = roc_auc_score(y_test, y_pred_proba)
    AUCPR = average_precision_score(y_test, y_pred_proba)
    P = precision_score(y_test, y_pred)
    R = recall_score(y_test, y_pred)
    F1 = f1_score(y_test, y_pred)

    print(f"Accuracy: {ACC}; AUC: {AUC}; AUCPR: {AUCPR}; P: {P}; R: {R}; F1: {F1}")
    
    return ACC, AUC, AUCPR, P, R, F1

# Debug Function
def count_rlf(data_list):
    count = 0
    for f in data_list:
        df = pd.read_csv(f)
        for i in range(len(df)):
            if df['RLF_II'].iloc[i] or df['RLF_III'].iloc[i]:
                count += 1
    return count

def np_ary_to_df(arr, col_names):
    df = pd.DataFrame(arr, columns=col_names)
    return df
    
def find_original_input(ind, file_record, time_seq_len, ffill_cols):
    for (file, ind_range) in file_record:
        if ind_range[0]<=ind<ind_range[1]:
            target_file = file    
            tar_ind_range = ind_range
            
    df = pd.read_csv(target_file)
    df[ffill_cols] = df[ffill_cols].replace(0, pd.NA)
    df[ffill_cols] = df[ffill_cols].ffill()
    for col in ffill_cols:
        if not pd.notna(df[col].iloc[0]):
            df = df[df[col].notna()]
    df.reset_index(drop=True, inplace=True)
    return df[ind-tar_ind_range[0]:ind-tar_ind_range[0]+time_seq_len], target_file

def get_pred_result_ind(model, x, labels, X):
    TP, FP, TN, FN = [], [], [], [] 

    y_pred_proba = model.predict(x) 
    y_pred = (y_pred_proba > 0.5).astype(int)
    
    for i, (pred, label, x) in enumerate(zip(y_pred, labels, X)):
        if pred != label:
            if label == 1: # FP analysis
                FP.append(i)
            else: # FN analysis
                FN.append(i)
        else: 
            if label == 1: # TP analysis
                TP.append(i)
            else:
                TN.append(i)        
    return TP, FP, TN, FN

def split_train_valid(l, ratio=0.8):
    n = int(len(l)*ratio)
    train = [l[i] for i in range(n)]
    valid = [l[i] for i in range(n, len(l))]
    return train, valid

if __name__ == "__main__":
    features = ['num_of_neis', 'RSRP','RSRQ','RSRP1','RSRQ1','nr-RSRP','nr-RSRQ','nr-RSRP1','nr-RSRQ1',
                'E-UTRAN-eventA3','eventA5','NR-eventA3','eventB1-NR-r15',
                'LTE_HO','MN_HO','MN_HO_to_eNB','SN_setup','SN_Rel','SN_HO', 
                'RLF_II', 'RLF_III','SCG_RLF']
    ffill_cols = ['RSRP1', 'RSRQ1']
    two_hot_vec_cols = ['E-UTRAN-eventA3','eventA5','NR-eventA3','eventB1-NR-r15',
                'LTE_HO','MN_HO','MN_HO_to_eNB','SN_setup','SN_Rel','SN_HO','RLF_II','RLF_III','SCG_RLF']
    merged_cols = [['LTE_HO', 'MN_HO_to_eNB', 'LTE_HO'], ['RLF_II', 'RLF_III', 'RLF']]
    data_list = ['/home/wmnlab/Documents/r11921052/曾聖儒/test/2023-03-26_qc00_UDP_Bandlock_All_RM500Q_#02_All.csv']
    ts_array_create(data_list,10,10,features,ffill_cols,two_hot_vec_cols,merged_cols)
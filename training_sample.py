#%%
import os
import sys 
sys.path.insert(1,os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database.db_api import *
from multiprocessing.pool import ThreadPool as Pool
from create_training_data_v2 import *
from imblearn.over_sampling import SMOTE
from imblearn.over_sampling import ADASYN
from collections import Counter
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
# import tensorflow as tf
from sklearn.metrics import r2_score
import shutil
import yaml
import datetime as dt
import argparse
from sklearn.metrics import classification_report
import xgboost as xgb
import random
import optuna
from sklearn.metrics import f1_score
from autogluon.tabular import TabularPredictor
#%%
random.seed(42)

LOGDIR = "tf_log_regr"
SESSION_DIR = ""
RESULT_DIR = ""


class HoData:
    def __init__(self, db_list, interval, num_of_x_seq, num_of_y_seq, target = [PredictTarget.RLF]):
        results = []
        self.train_x = []
        self.train_y = []
        self.test_x = []
        self.test_y = []
        self.valid_x = []
        self.valid_y = []
        self.features = None
        self.rand_state = 85642
        with Pool(processes=20) as pool:
            for db_file in db_list:
                results.append(pool.apply_async(create_train_data, args=(db_file, interval, num_of_x_seq, num_of_y_seq,target, LabelType.REGRESSION, EncodeType.LABEL)))
                
            for result in results:
                x, y, f = result.get()
                if x is None:
                    continue
                if self.features == None:
                    self.features = f
                self.train_x.append(x[:,:,18:])
                self.train_y.append(y)
        self.train_x = np.concatenate(self.train_x, axis=0)
        # self.train_x = np.transpose(self.train_x, (1, 0, 2))
        
        self.train_y = np.concatenate(self.train_y, axis=0)
        self.train_y = np.squeeze(self.train_y, axis=2)
        
        # self.train_x, self.train_y = downsample_zeros(self.train_x, self.train_y, downsample_rate=0)
        
        print(np.isnan(self.train_x).any())
        print(np.isnan(self.train_y).any())
        print(self.train_x.shape)
        print(self.train_y.shape)
        print("Total number of test sequences: {}".format(self.train_x.shape[1]))
        total_seqs = self.train_x.shape[0]
        valid_size = int(0.2*total_seqs)
        test_size = int(0.1*total_seqs)
        train_size = total_seqs - valid_size - test_size
        
        self.valid_x = self.train_x[:valid_size]
        self.valid_y = self.train_y[:valid_size]

        self.test_x = self.train_x[valid_size:valid_size + test_size]
        self.test_y = self.train_y[valid_size:valid_size + test_size]

        self.train_x = self.train_x[valid_size + test_size:]
        # self.train_x_label = self.train_x[valid_size + test_size:]
        self.train_y = self.train_y[valid_size + test_size:]
        
        # self.train_x, self.train_y = self.shuffle(self.train_x, self.train_y)
        # self.valid_x, self.valid_y = self.shuffle(self.valid_x, self.valid_y)
        # self.test_x, self.test_y = self.shuffle(self.test_x, self.test_y)
        
    def shuffle(self, x, y):
        total_seqs = x.shape[0]
        permutation = np.random.RandomState(self.rand_state).permutation(total_seqs)
        self.rand_state+=1
        return x[permutation], y[permutation]
    def iterate_train(self, data_x, data_y, batch_size=16):
        total_seqs = data_x.shape[0]
        permutation = np.random.permutation(total_seqs)
        total_batches = total_seqs // batch_size

        for i in range(total_batches):
            start = i*batch_size
            end = start + batch_size
            batch_x = data_x[permutation[start:end]]
            batch_y = data_y[permutation[start:end]]
            yield (batch_x,batch_y)

def get_db_list():
    db_files = []
    db = duckdb.connect("/home/wmnlab/Documents/r12921063/database/main_record.db")
    location_list = list(db.sql("""
    select route, location from expr where 
    name not like '%Modem_Action%' and
    route = 'BR' and
    band = 'All' and
    location not in (select location from expr where name LIKE 'UDP-450sec' and device LIKE 'sm00');
    """).df()['location'])
    for i in location_list:
        db_files.append(location_to_db_name('BR', i))
    db.close()
    return db_files

def get_db_list2(route):
    db_files = []
    db = duckdb.connect("/home/wmnlab/Documents/r12921063/database/main_record.db")
    location_list = list(db.sql(f"""
    select route, location from expr where 
    name not like '%Modem_Action%' and
    route = '{route}';
    """).df()['location'])
    for i in location_list:
        db_files.append(location_to_db_name('A', i))
    db.close()
    return db_files
import subprocess
def help(route):

    # 執行 du 指令
    result = subprocess.run(
        f'du -sh /home/wmnlab/Documents/r12921063/database/{route}/*',
        shell=True,
        stdout=subprocess.PIPE,
        text=True
    )

    # 分析輸出並過濾大於 12K 的項目
    file_list = []
    for line in result.stdout.strip().split('\n'):
        print(line)
        try:
            # 分割大小和路徑
            size_str, path = line.split('\t')
        except ValueError:
            print(f"Skipping line due to unexpected format: {line}")
            continue
        # size_str, path = line.split('\t')

        # 將大小轉換為 K 單位的數字
        if size_str.endswith('K'):
            size = float(size_str[:-1])
        elif size_str.endswith('M'):
            size = float(size_str[:-1]) * 1024
        elif size_str.endswith('G'):
            size = float(size_str[:-1]) * 1024 * 1024
        else:
            continue  # 忽略無法解析的格式

        if size > 12:
            file_list.append(path)
        else:
            print(f"File {path} is smaller than 12K, size: {size}K")

    return file_list
def create_log_dir(args):
    global LOGDIR, SESSION_DIR, RESULT_DIR
    log_dir = f"{LOGDIR}/{dt.date.today().strftime('%Y-%m-%d')}/"
    print(f"Log dir is {log_dir}")
    for i in range(100):
        if os.path.exists(log_dir+f'{i}/'):
            continue
        if not os.path.exists(log_dir+f'{i}/'):
            log_dir = log_dir+f'{i}/'
            os.umask(0)
            os.makedirs(log_dir + f"result")
            os.makedirs(log_dir + f"script")
            os.makedirs(log_dir + f"session")
            RESULT_DIR = log_dir + f"result"
            SESSION_DIR = log_dir + f"session"
            with open(log_dir + "args.yaml", "w", encoding="utf-8") as file:
                yaml.dump(vars(args), file, allow_unicode=True)
            shutil.copyfile(os.path.abspath(__file__), log_dir + f"script/" + os.path.basename(os.path.abspath(__file__)))
            break

# %%
if __name__ == "__main__":
    target = [PredictTarget.RLF]
    model_type = 'ltc'
    model_size = 64
    interval = 0.1
    time_steps_x = int(3/interval)
    time_steps_y = int(3/interval)
    
    num_of_features = None
    num_classes = 2
    learning_rate = 0.001
    epochs = 100
    batch_size = 32
    # if not args.test:
    #     create_log_dir(args)
    #%%
    directory="/home/wmnlab/Documents/r12921063/database"
    db_list = get_db_list()
    db_list = help("HSR")
    #%%
    if len(db_list) > 300:
        db_list=random.sample(db_list,300)
    random.shuffle(db_list)
    ho_data = HoData(db_list, interval, time_steps_x, time_steps_y, target)
    num_of_features = ho_data.train_x.shape[2]
    SESSION_DIR = '/home/wmnlab/Documents/r12921063/training/tf_log_regr/2024-11-13/0/session'
#%%
params = {
    'random_seed': 7,
    'objective': 'binary:logistic',  # ⚠️ 分類任務建議用
    'eval_metric': 'auc',
    'max_depth': 8,
    'learning_rate': 0.01,
    'subsample': 0.8,
    'colsample_bytree': 0.7,
    'alpha': 0.1,
    'gamma': 5,
    'lambda': 1.0,
    'tree_method': 'hist',
    'device': 'cuda:0',
    'min_child_weight': 10,
    'scale_pos_weight': 10,  # 建議程式計算
}
params = {
    'max_depth': 10, 
    'max_bin': 91, 
    'subsample': 0.8476878915637002, 
    'colsample_bytree': 0.6639322782325201, 
    'min_child_weight': 23, 
    'learning_rate': 0.05731800650236085, 
    'lambda': 0.101144654374409, 
    'alpha': 4.237422418210531,
    'scale_pos_weight': 10.223953354684767,
    "objective": "binary:logistic",  # 根據你的任務換成合適目標函數
    "eval_metric": "auc",             # 或其他你想用的評估指標
    'random_seed': 7,
    'nthread': 4,
}

params = {
    'max_depth': 10,
    'max_bin': 123,
    'subsample': 0.7670747330805957,
    'colsample_bytree': 0.7548874006391285,
    'min_child_weight': 26,
    'learning_rate': 0.08517306773128662,
    'lambda': 0.4876445201375328,
    'alpha': 3.906025400365329,
    'scale_pos_weight': 11.175346531657272,
    "objective": "binary:logistic",  # 根據你的任務換成合適目標函數
    "eval_metric": "auc",             # 或其他你想用的評估指標
    'random_seed': 7,
    'nthread': 4,
}
#%%
# ls = 3
# db_list = get_db_list()
# random.shuffle(db_list)
# chunk_size = 100
# num_chunks = (len(db_list) + chunk_size - 1) // chunk_size
# print(f"num of chunks {num_chunks}")
# model = None
# for index in range(num_chunks):
#     start = index * chunk_size
#     chunk = db_list[start:start + chunk_size]
#     ho_data = HoData(chunk, interval, time_steps_x, time_steps_y, target)
#     print(f"loading chunk {index} finish")
#     dtrain = xgb.DMatrix(ho_data.train_x.reshape(ho_data.train_x.shape[0], -1), label=np.where(np.squeeze(ho_data.train_y ) < ls, 1, 0))
#     dvalid = xgb.DMatrix(ho_data.valid_x.reshape(ho_data.valid_x.shape[0], -1), label=np.where(np.squeeze(ho_data.valid_y ) < ls, 1, 0))
    
#     num_rounds = 2000
#     watchlist = [(dvalid,'valid')] 
#     if index == num_chunks - 1:
#         # Special processing for the last chunk
#         dtest = xgb.DMatrix(ho_data.train_x.reshape(ho_data.train_x.shape[0], -1), label=np.where(np.squeeze(ho_data.train_y ) < ls, 1, 0))
#         y_pred_test = model.predict(dtest)
#         t = 1.5
#         print(classification_report(np.where(np.squeeze(ho_data.train_y ) < t, 1, 0), y_pred_test > 0.5))
#     else:
#         print(f"Chunk {index+1}:", chunk)
#         watchlist = [(dvalid,'valid')] 
#         if model != None:
#             # params['updater'] = 'grow_colmaker'
#             params['process_type'] = 'default'
#         if  model != None and num_rounds > model.best_iteration:
#             num_rounds = min(200, model.best_iteration)
#         model = xgb.train(params, dtrain, num_rounds, evals=watchlist, early_stopping_rounds=30,  verbose_eval=True, xgb_model=model)
#         # Regular processing for other chunks
#         y_pred_valid = model.predict(dvalid)
#         t = 1.5
#         print(classification_report(np.where(np.squeeze(ho_data.valid_y ) < t, 1, 0), y_pred_valid > 0.5))
#%%
# xgb parameters
from sklearn.metrics import f1_score, recall_score, precision_score
def multi_metric_eval(preds, dtrain):
    labels = dtrain.get_label()
    preds_binary = (preds > 0.5).astype(int)

    f1 = f1_score(labels, preds_binary)
    recall = recall_score(labels, preds_binary)
    precision = precision_score(labels, preds_binary)

    return [
        ('f1', f1),
        ('recall', recall),
        ('precision', precision)
    ]
params = {
    'max_depth': 10, 
    'max_bin': 91, 
    'subsample': 0.8476878915637002, 
    'colsample_bytree': 0.6639322782325201, 
    'min_child_weight': 23, 
    'learning_rate': 0.05731800650236085, 
    'lambda': 0.101144654374409, 
    'alpha': 4.237422418210531,
    'scale_pos_weight': 10.223953354684767,
    "objective": "binary:logistic",  # 根據你的任務換成合適目標函數
    "eval_metric": "auc",             # 或其他你想用的評估指標
    'random_seed': 7,
    'nthread': 4,
}

params = {
    'max_depth': 10,
    'max_bin': 79,
    'subsample': 0.637996970227577,
    'colsample_bytree': 0.881495099137233,
    'min_child_weight': 18,
    'learning_rate': 0.07459937618994596,
    'lambda': 4.26638478654916,
    'alpha': 0.203810346045612,
    'scale_pos_weight': 10.641937463639303,
    "objective": "binary:logistic",  # 根據你的任務換成合適目標函數
    "eval_metric": "auc",             # 或其他你想用的評估指標
    'random_seed': 7,
    'nthread': 4,
}
# params = {
#     'max_depth': 10,
#     'max_bin': 123,
#     'subsample': 0.7670747330805957,
#     'colsample_bytree': 0.7548874006391285,
#     'min_child_weight': 26,
#     'learning_rate': 0.08517306773128662,
#     'lambda': 0.4876445201375328,
#     'alpha': 3.906025400365329,
#     'scale_pos_weight': 11.175346531657272,
#     "objective": "binary:logistic",  # 根據你的任務換成合適目標函數
#     "eval_metric": "auc",             # 或其他你想用的評估指標
#     'random_seed': 7,
#     'nthread': 4,
# }
params = {
    'max_depth': 5,
    'max_bin': 85,
    'subsample': 0.6645385027823321,
    'colsample_bytree': 0.7139321499446738,
    'min_child_weight': 21,
    'learning_rate': 0.062147292558073115,
    'lambda': 3.7904773567551286,
    'alpha': 3.2116713254650917,
    'scale_pos_weight': 50.948644649489026,
    "objective": "binary:logistic",  # 根據你的任務換成合適目標函數
    "eval_metric": "auc",             # 或其他你想用的評估指標
    'random_seed': 7,
    'nthread': 4,
}
t = 3
dtrain = xgb.DMatrix(ho_data.train_x.reshape(ho_data.train_x.shape[0], -1), label=np.where(np.squeeze(ho_data.train_y ) < t, 1, 0))
dvalid = xgb.DMatrix(ho_data.valid_x.reshape(ho_data.valid_x.shape[0], -1), label=np.where(np.squeeze(ho_data.valid_y ) < t, 1, 0))
dtest = xgb.DMatrix(ho_data.test_x.reshape(ho_data.test_x.shape[0], -1), label=np.where(np.squeeze(ho_data.test_y ) < t, 1, 0))
num_rounds = 2000
watchlist = [(dvalid,'valid')] 
model = xgb.train(params, dtrain, num_rounds, evals=watchlist, early_stopping_rounds=30,  verbose_eval=True)
y_pred_valid = model.predict(dvalid)
y_pred_test = model.predict(dtest)
# print(classification_report(np.where(np.squeeze(ho_data.train_y ) < 3, 1, 0), y_pred_train > 0.5))
print(classification_report(np.where(np.squeeze(ho_data.valid_y ) < t, 1, 0), y_pred_valid > 0.5))
print(classification_report(np.where(np.squeeze(ho_data.test_y ) < t, 1, 0), y_pred_test > 0.5))

# cv_results = xgb.cv(
#     params=params,
#     dtrain=dtrain,
#     num_boost_round=num_rounds,
#     nfold=5,
#     stratified=True,  # 如果是分類問題建議設為 True
#     early_stopping_rounds=30,
#     feval=multi_metric_eval,
#     verbose_eval=True,
#     seed=42, # 為了重現性
#     shuffle=False
# )
#%%
import numpy as np
from sklearn.metrics import f1_score, recall_score, precision_score
from sklearn.model_selection import KFold
import xgboost as xgb

# 將 label 設定為二元分類：小於 t 則為 1，否則為 0
t = 2
X = ho_data.train_x.reshape(ho_data.train_x.shape[0], -1)
y = np.where(np.squeeze(ho_data.train_y) < t, 1, 0)

# 你提供的 XGBoost 參數
params = {
    'max_depth': 10, 
    'max_bin': 91, 
    'subsample': 0.8476878915637002, 
    'colsample_bytree': 0.6639322782325201, 
    'min_child_weight': 23, 
    'learning_rate': 0.05731800650236085, 
    'lambda': 0.101144654374409, 
    'alpha': 4.237422418210531,
    'scale_pos_weight': 10.223953354684767,
    "objective": "binary:logistic",  # 根據你的任務換成合適目標函數
    "eval_metric": "auc",             # 或其他你想用的評估指標
    'random_seed': 7,
    'nthread': 4,
}
params = {
    'max_depth': 10,
    'max_bin': 123,
    'subsample': 0.7670747330805957,
    'colsample_bytree': 0.7548874006391285,
    'min_child_weight': 26,
    'learning_rate': 0.08517306773128662,
    'lambda': 0.4876445201375328,
    'alpha': 3.906025400365329,
    'scale_pos_weight': 11.175346531657272,
    "objective": "binary:logistic",  
    "eval_metric": "auc",            
    'random_seed': 7,
    'nthread': 4,
}
params = {
    'max_depth': 10,
    'max_bin': 79,
    'subsample': 0.637996970227577,
    'colsample_bytree': 0.881495099137233,
    'min_child_weight': 18,
    'learning_rate': 0.07459937618994596,
    'lambda': 4.26638478654916,
    'alpha': 0.203810346045612,
    'scale_pos_weight': 10.641937463639303,
    "objective": "binary:logistic",  # 根據你的任務換成合適目標函數
    "eval_metric": "auc",             # 或其他你想用的評估指標
    'random_seed': 7,
    'nthread': 4,
}
# 自訂 metrics
def multi_metric_eval(preds, dtrain):
    labels = dtrain.get_label()
    preds_binary = (preds > 0.5).astype(int)

    f1 = f1_score(labels, preds_binary)
    recall = recall_score(labels, preds_binary)
    precision = precision_score(labels, preds_binary)

    return [
        ('f1', f1),
        ('recall', recall),
        ('precision', precision)
    ]

# 開始進行自訂 5-fold CV
num_rounds = 100
skf = KFold(n_splits=5, shuffle=False)

fold = 1
all_results = []
rand_state = 42
for train_idx, valid_idx in skf.split(X, y):
    random.shuffle(train_idx)
    random.shuffle(valid_idx)
    X_train, X_valid = X[train_idx], X[valid_idx]
    y_train, y_valid = y[train_idx], y[valid_idx]

    dtrain = xgb.DMatrix(X_train, label=y_train)
    dvalid = xgb.DMatrix(X_valid, label=y_valid)
    print(f"\n[Fold {fold}]")
    print(f"Train size: {len(y_train)}, y=1: {np.sum(y_train)}, ratio: {np.mean(y_train):.4f}")
    print(f"Valid size: {len(y_valid)}, y=1: {np.sum(y_valid)}, ratio: {np.mean(y_valid):.4f}")

    model = xgb.train(
        params=params,
        dtrain=dtrain,
        num_boost_round=num_rounds,
        evals=[(dvalid, 'valid')],
        early_stopping_rounds=30,
        verbose_eval=False
    )
    # print()

    preds = model.predict(dvalid)
    preds_binary = (preds > 0.5).astype(int)

    f1 = f1_score(y_valid, preds_binary)
    recall = recall_score(y_valid, preds_binary)
    precision = precision_score(y_valid, preds_binary)
    # y_pred_test = model.predict(dtest)
    # print(classification_report(np.where(np.squeeze(ho_data.test_y ) < t, 1, 0), y_pred_test > 0.5))
    all_results.append({'fold': fold, 'f1': f1, 'recall': recall, 'precision': precision})
    print(f"[Fold {fold}] F1: {f1:.4f}, Recall: {recall:.4f}, Precision: {precision:.4f}")
    fold += 1

#%%
t = 2  # threshold to binarize regression targets

# 資料預處理（照你的方式 reshape + label 二值化）
def make_dmatrix(x, y, threshold=2.5):
    x_reshaped = x.reshape(x.shape[0], -1)
    y_binary = np.where(np.squeeze(y) < threshold, 1, 0)
    return xgb.DMatrix(x_reshaped, label=y_binary)

# 準備資料
dtrain = make_dmatrix(ho_data.train_x, ho_data.train_y, threshold=t)
dvalid = make_dmatrix(ho_data.valid_x, ho_data.valid_y, threshold=t)

# Ground truth for validation (for metric)
y_val = np.where(np.squeeze(ho_data.valid_y) < t, 1, 0)

# Optuna 調參目標函數
def objective(trial):
    try:
        params = {
            'objective': 'binary:logistic',
            'eval_metric': 'auc',
            'tree_method': 'hist',   # 或 'gpu_hist'（有些版本寫成 hist + device:cuda）
            'device': 'cuda',
            'max_depth': trial.suggest_int('max_depth', 4, 10),
            'max_bin': trial.suggest_int('max_bin', 64, 128),
            'grow_policy': 'lossguide',
            'subsample': trial.suggest_float('subsample', 0.6, 0.9),
            'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 0.9),
            'min_child_weight': trial.suggest_int('min_child_weight', 10, 30),
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.1),
            'lambda': trial.suggest_float('lambda', 0.1, 5.0),
            'alpha': trial.suggest_float('alpha', 0.1, 5.0),
            'scale_pos_weight': trial.suggest_float('scale_pos_weight', 1, 500),
        }

        booster = xgb.train(
            params,
            dtrain,
            num_boost_round=1000,
            evals=[(dvalid, "validation")],
            early_stopping_rounds=50,
            verbose_eval=False
        )

        y_pred = booster.predict(dvalid)
        y_pred_label = (y_pred > 0.5).astype(int)

        return f1_score(y_val, y_pred_label)
    except xgb.core.XGBoostError as e:
        print("Trial failed due to memory error.")
        return 0.0
# 開始調參
study = optuna.create_study(direction="maximize")
study.optimize(objective, n_trials=50)
#%%
# 結果
print("Best F1-score:", study.best_value)
print("Best parameters:", study.best_params)

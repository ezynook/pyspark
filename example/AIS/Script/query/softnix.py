from time import time
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

def reduce(df):
    start_mem = df.memory_usage().sum() / 1024**2
    for col in df.columns:
        col_type = df[col].dtype
        
        if col_type != object and col_type.name != 'category' and 'datetime' not in col_type.name:
            c_min = df[col].min()
            c_max = df[col].max()
            if str(col_type)[:3] == 'int':
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)  
            else:
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    df[col] = df[col].astype(np.float16)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)
        elif 'datetime' not in col_type.name:
            df[col] = df[col].astype('category')

    end_mem = df.memory_usage().sum() / 1024**2
    return df

def duration_ms(stime: str, etime: str):
	start_time = stime
	end_time = etime
	t1 = datetime.strptime(start_time, "%H:%M:%S")
	t2 = datetime.strptime(end_time, "%H:%M:%S")
	delta = t2 - t1
	ms = delta.total_seconds() * 1000
	return int(ms)

def duration_s(stime: str, etime: str):
	start_time = stime
	end_time = etime
	t1 = datetime.strptime(start_time, "%H:%M:%S")
	t2 = datetime.strptime(end_time, "%H:%M:%S")
	delta = t2 - t1
	ms = delta.total_seconds()
	return int(ms)

def cgn_read(sql):
	print("+-----Softnix Read SQL AIS CGN Log-------+")
	engine = create_engine('trino://root:@trinoserv.bigdata:8090/hive/ais')
	connection = engine.connect()
	df = pd.read_sql(sql, con=engine)
	result = reduce(df)
	del df
	return result


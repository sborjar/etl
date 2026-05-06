import numpy as np
import pandas as pd
import os
import time
from src.funcs import log,logT
from src.load import loadDB
from src.config import Config

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def transform(df, deep=0):
    """Transform and aggregate data.

        Args:
            df: Input DataFrame 
            deep: Aggregation level (0 = daily, 1 = monthly)

        Returns:
            Transformed DataFrame
        """
    
    log(f' BEGIN TRANSFORM', "", 1)
    
    """ Valitate is empty the data frame """
    if df is None or df.empty:
        log(f" The Data frame comes empty", "error")
        return None
    
    
    """ Start time to elapsed """
    start_time = time.perf_counter()
    

    """ Validation when calldate is NAT or not valid """
    initial_rows = len(df)
    df.dropna(subset=['calldate'], inplace=True)
    dropped_rows = initial_rows - len(df)
    if dropped_rows > 0:
        log(f" Dropped {dropped_rows} rows with invalid dates.")
    
    log(f" Make sure there are no null values in numeric cols")
    """ Make sure there are no null values in numeric cols """
    numeric_cols = ['billsec', 'waiting', 'talked', 'wrapped', 'sla', 'dispositioned']
    for col in numeric_cols:
        if col in df.columns:
            # fillna(0) asegura que no haya NaN, permitiendo convertir a float64/int64 estándar
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
    """ Ensure the calldate is datetime format """
    log(f" Ensure the calldate is datetime format ")
    df["calldate"] = pd.to_datetime(df["calldate"], errors='coerce')

    """ Add year, month, day columns from calldate. """
    log(f" Add year, month, day columns from calldate.")
    # Ahora sí es seguro convertir a int32 porque no hay NaN en calldate
    df["year"] =  df['calldate'].dt.year.astype(np.int32)
    df["month"] =  df['calldate'].dt.month.astype(np.int32)
    df["day"] =  df['calldate'].dt.day.astype(np.int32)
    
    """ UNITS """
    log(f" Units calculation")
    df['units_calc'] = np.where(
        df['billsec'] == 0,
        0,
        np.maximum(3, np.ceil(df['billsec'] / 6 * 1.3))
    )
    
    # if 'agentid' in df.columns:
    #     if df['agentid'].dtype == 'object':
    #         df['agentid'] = df['agentid'].fillna('UNKNOWN')
    #     else:
    #         df['agentid'] = df['agentid'].fillna(-1).astype(np.int64)

    
    if 'callid' in df.columns:
            df['callid'] = df['callid'].fillna(0) 
    
    """ Conditions """
    df['agenthandled_calc'] = np.where(df["agentid"] > 0, 1, 0)
    df['noanswers_calc'] = np.where(df["callresult"] == 2, 1, 0)
    df['busy_calc'] = np.where(df["callresult"] == 3, 1, 0)
    df['oi_calc'] = np.where(df["callresult"] == 4, 1, 0)
    df['drops_calc'] = np.where(df["callresult"] == 5, 1, 0)
    df['amd_calc'] = np.where(df["callresult"] == 6, 1, 0)
    df['others_calc'] = np.where((df["callresult"] < 1) & (df['callresult'] > 6), 1, 0)
    df['contact_calc'] = np.where((df["callresult"] == 1) & (df['contact'] == 'y'),1,0)
    df['success_calc'] = np.where((df["callresult"] == 1) & (df['success'] == 'y'),1,0)

    log(f" Grouping")
    
    if deep == 0:
        log(f" Grouping by Day")
        header_group =  ['tenantid', 'camp_id', 'year', 'month', 'day']
    elif deep == 1:
        header_group =  ['tenantid', 'camp_id', 'year', 'month']
    
    
    df_day = df.groupby(
        header_group,
        as_index=False,
        sort=False
    ).agg(
        agents=('agentid', 'nunique'),
        # totalcalls=('callid', 'count'),
        agenthandled=('agenthandled_calc', 'sum'),
        noanswers=('noanswers_calc', 'sum'),
        busy=('busy_calc', 'sum'),
        oi=('oi_calc', 'sum'),
        drops=('drops_calc', 'sum'),
        amd=('amd_calc', 'sum'),
        others=('others_calc', 'sum'),
        contacts=('contact_calc', 'sum'),
        success=('success_calc', 'sum'),
        waiting=('waiting', 'sum'),
        talked=('talked', 'sum'),
        wrapped=('wrapped', 'sum'),
        sla=('sla', 'sum'),
        # dispositioned=('dispositioned', lambda x: x.astype(bool).sum()),
        billsec=('billsec', lambda x: (x * 1.3).sum()),
        units=('units_calc', 'sum')
        # agenthandled=('agentid', lambda x: (x > 0).sum()),
        # noanswers=('callresult', lambda x: (x == 2).sum()),
        # busy=('callresult', lambda x: (x == 3).sum()),
        # oi=('callresult', lambda x: (x == 4).sum()),
        # drops=('callresult', lambda x: (x == 5).sum()),
        # amd=('callresult', lambda x: (x == 6).sum()),
        # others=('callresult', lambda x: (~x.isin([1, 2, 3, 4, 5, 6])).sum()),
        # contacts=('contact_calc', 'sum'),
        # success=('success_calc', 'sum'),
        # waiting=('waiting', 'sum'),
        # talked=('talked', 'sum'),
        # wrapped=('wrapped', 'sum'),
        # sla=('sla', 'sum'),
        # # dispositioned=('dispositioned', lambda x: x.astype(bool).sum()),
        # billsec=('billsec', lambda x: (x * 1.3).sum()),
        # units=('billsec', unitsCall)
    )

    end_time = time.perf_counter()
    elapsed_time_transform = end_time - start_time
    print(df_day.head(5))
    logT('TRANSFORM',df_day.shape[0],elapsed_time_transform)
    
    if deep == 0:
        log(f" Elapsed TRANSFORM {elapsed_time_transform:.4f} seconds ")
    elif deep == 1:
        log(f" Elapsed SUMMARY TRANSFORM {elapsed_time_transform:.4f} seconds ")
    

    # file_path = Config.DATA_DIR / f"transform.csv"
    # df_day.to_csv(file_path, mode='w',  lineterminator='\n', encoding='latin1', index=False)
    
    log(f' END TRANSFORM', "", 1)
    loadDB(df_day, deep)
    
    
# def unitsCall(billsec_series: pd.Series):
#     total = 0.0
#     for billsec in billsec_series:
#         if billsec == 0:
#             continue
#         total += max(3, int(np.ceil(billsec / 6 * 1.3)))
#     return total
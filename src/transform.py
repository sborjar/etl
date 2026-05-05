import numpy as np
import pandas as pd
import os
import time
import polars as pl
from src.funcs import log,logT
from src.load import loadDB

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def transform(df, deep=0):
    """Transform and aggregate data.

        Args:
            df: Input DataFrame (pandas, polars, or pyspark)
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
    
    log(f" Condition for callresult 1 and 5 ")
    # Condition for callresult 1 and 5 
    df['is_agent_call'] = (df['callresult'] == 1).astype(np.int8)
    df['is_drop']       = (df['callresult'] == 5).astype(np.int8)

    log(f" Grouping")
    
    if deep == 0:
        log(f" Grouping by Day")
        df_day = df.groupby(['tenantid', 'camp_id', 'year', 'month', 'day']).agg(
            agents=('agentid', 'nunique'),
            totalcalls=('callid', 'count'),
            totalagentcalls=('is_agent_call', 'sum'),
            totaldrops=('is_drop', 'sum'),
            billsec=('billsec', 'sum'),
            units=('units_calc', 'sum'),
            waiting=('waiting', 'sum'),
            talked=('talked', 'sum'),
            wrapped=('wrapped', 'sum'),
            sla=('sla', 'sum'),
            dispositioned=('dispositioned', 'sum')
        ).reset_index()
        
        df_day['billsec'] *= 1.3

    elif deep == 1:
        log(f" Preparing DataFrame for Polars conversion")
        
        # --- LIMPIEZA FINAL PARA POLARS ---
        # Polars es estricto con los tipos. Aseguramos que no queden NaNs en columnas clave.
        
        # 1. Limpiar agentid para el nunique
        if 'agentid' in df.columns:
            # Si agentid tiene NaN, nunique los ignora por defecto en Pandas, 
            # pero en Polars puede ser tricky si el tipo es object con None.
            # Lo más seguro es rellenar con un placeholder si es string, o 0 si es numérico.
            if df['agentid'].dtype == 'object':
                df['agentid'] = df['agentid'].fillna('UNKNOWN')
            else:
                df['agentid'] = df['agentid'].fillna(-1).astype(np.int64)
        
        # 2. Asegurar que callid no tenga NaNs para el count
        if 'callid' in df.columns:
             df['callid'] = df['callid'].fillna(0) # O algún ID dummy

        # 3. Convertir a Polars
        try:
            df_pl = pl.from_pandas(df)
        except Exception as e:
            log(f" Error converting to Polars: {e}")
            log(" Fallback: Printing dtypes to debug")
            log(df.dtypes)
            raise e

        log(f" Grouping by Month with Polars")
        # Sintaxis correcta de Polars para agg
        df_day_pl = df_pl.group_by(['tenantid', 'camp_id', 'year', 'month']).agg([
            pl.col('agentid').n_unique().alias('agents'),
            pl.col('callid').count().alias('totalcalls'),
            pl.col('is_agent_call').sum().alias('totalagentcalls'),
            pl.col('is_drop').sum().alias('totaldrops'),
            pl.col('billsec').sum().alias('billsec'),
            pl.col('units_calc').sum().alias('units'),
            pl.col('waiting').sum().alias('waiting'),
            pl.col('talked').sum().alias('talked'),
            pl.col('wrapped').sum().alias('wrapped'),
            pl.col('sla').sum().alias('sla'),
            pl.col('dispositioned').sum().alias('dispositioned')
        ])
        
        df_day = df_day_pl.to_pandas()
        df_day['billsec'] *= 1.3

    end_time = time.perf_counter()
    elapsed_time_transform = end_time - start_time
    
    logT('TRANSFORM',df_day.shape[0],elapsed_total)
    
    if deep == 0:
        log(f" Elapsed TRANSFORM {elapsed_time_transform:.4f} seconds ")
    elif deep == 1:
        log(f" Elapsed SUMMARY TRANSFORM (Polars) {elapsed_time_transform:.4f} seconds ")
    
    log(f' END TRANSFORM', "", 1)
    
    loadDB(df_day, deep)
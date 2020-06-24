import pandas as pd

def mem_usage(pandas_obj):
    if isinstance(pandas_obj,pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else: # we assume if not a df it's a series
        usage_b = pandas_obj.memory_usage(deep=True)
    usage_mb = usage_b / 1024 ** 2 # convert bytes to megabytes
    return "{:03.2f} MB".format(usage_mb)

def optimize_int(df):
    df_int = df.select_dtypes(include=['int'])
    converted_int = df_int.apply(pd.to_numeric,downcast='unsigned')
    df[converted_int.columns] = converted_int
    return(df)

def optimize_float(df):
    df_float = df.select_dtypes(include=['float'])
    converted_float = df_float.apply(pd.to_numeric,downcast='float')
    df[converted_float.columns] = converted_float
    return(df)

def optimize_obj(df):
    converted_obj = pd.DataFrame()
    df_obj = df.select_dtypes(include=['object']).copy()
    for col in df_obj.columns:
        num_unique_values = len(df_obj[col].unique())
        num_total_values = len(df_obj[col])
        if num_unique_values / num_total_values < 0.5:
            converted_obj.loc[:,col] = df_obj[col].astype('category')
        else:
            converted_obj.loc[:,col] = df_obj[col]
    df[converted_obj.columns] = converted_obj
    return(df)

def optimize_all(df):
    df_opt = optimize_int(df)
    df_opt = optimize_float(df_opt)
    df_opt = optimize_obj(df_opt)
    return(df_opt)
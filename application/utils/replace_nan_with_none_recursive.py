import pandas as pd
import numpy as np

def replace_nan_with_none_recursive(obj):
    if isinstance(obj, dict):
        return {k: replace_nan_with_none_recursive(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_nan_with_none_recursive(elem) for elem in obj]
    elif isinstance(obj, float) and np.isnan(obj):
        return None
    elif pd.isna(obj): 
         return None
    else:
        return obj

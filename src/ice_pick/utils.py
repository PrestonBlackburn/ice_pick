from dataclasses import dataclass, field
from typing import List
import copy
import re
import configparser

from snowflake.snowpark import Session
import snowflake.snowpark as snowpark
from snowflake.snowpark.row import Row

import pandas as pd
import numpy as np


def snowpark_query(session, sql, non_select=False):
    """ 
    non-select queries include things like: 
     "show databases;"
    
    
    """
    
    if non_select:
        # apply the Row function "as_dict" to all rows then conver to pandas
        row_objs = session.sql(sql).collect()

        dict_array = np.array(list(map(Row.as_dict, row_objs)))

        df = pd.DataFrame.from_records(dict_array)

        return df
    
    else:
        
        df = session.sql(sql).to_pandas()
        
        return df
        
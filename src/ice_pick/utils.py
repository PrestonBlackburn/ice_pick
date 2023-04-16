from dataclasses import dataclass, field
from typing import List
import copy
import re
import configparser

from snowflake.snowpark import Session
import snowflake.snowpark as snowpark
from snowflake.snowpark.row import Row
from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, FloatType, NullType
from snowflake.snowpark.functions import lit

import pandas as pd
import numpy as np



# maybe add an option to log all sql executions 
# could be nice to have more transparency
# (at least add logging for debugging)
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
        

### Auto union
def _get_schemas(union_dfs:list) -> list:
    # get a flat list of all structs in all dataframes
    struct_field_list = []
    for df in  union_dfs:
        structs = df.schema.fields
        for struct in structs:
            struct_field_list.append(struct)
            
    return struct_field_list

def _get_unique_struct_fields(struct_field_list:list) -> list:
    # only get ge the unique structs
    unique_structs_list = []
    for struct in struct_field_list:
        if struct not in unique_structs_list:
            unique_structs_list.append(struct)
            
    return unique_structs_list

def _handle_struct_name_type_mismatch(unique_structs_list:list) -> list:
    # handle when columns with the same name have different data types
    # for now just pick the first data type, and discard the others
    unique_structs_type_list = []
    struct_name_list = []
    for struct in unique_structs_list:
        if struct.name not in struct_name_list:
            struct_name_list.append(struct.name)
            unique_structs_type_list.append(struct)
            
    return unique_structs_type_list


def _create_union_dataframe(session:Session, unique_structs_type_list:list) -> snowpark.DataFrame:
    # create the union dataframe from the unique schema structs
    union_schema = StructType(unique_structs_type_list)
    
    union_df = session.create_dataframe([], union_schema)
    
    return union_df


def _extend_schema(df: snowpark.DataFrame, union_df:snowpark.DataFrame) -> snowpark.DataFrame:
    
    col_names =  {struct.name: struct for struct in df.schema.fields}
    union_col_names = {struct.name: struct for struct in union_df.schema.fields}
    
    # get struct for col not in the dataframe
    structs_to_null = []
    for key in union_col_names:
        if key not in col_names:
            structs_to_null.append(union_col_names[key])
    
    # add struct as null col
    for struct in structs_to_null:
        
        df = df.with_column(struct.name, lit(None))
        
        # cast to the correct data type
        
    
    return df
    

def _add_null_cols(union_dfs:list, union_df: snowpark.DataFrame) -> list:
    # add null columns for the inclusive case
    # (the number of columns must match between tables, so null cols need to be added)
    ext_dfs = []
    for df in union_dfs:
        df = _extend_schema(df, union_df)
        ext_dfs.append(df)
    
    return ext_dfs



def _populate_union_df(ext_dfs:list, union_df: snowpark.DataFrame) -> snowpark.DataFrame:
    
    for df in ext_dfs:
        union_df = union_df.union_all_by_name(df)
  
    return union_df

# Might move these to a "pandas_func" module
def concat_standalone(session:Session, union_dfs:list) -> snowpark.DataFrame:
    """
    Returns a unioned dataframe from the input list of dataframes based on column names. 
    Primarly to handle cases where the number of columns do not match, 
    which is not suppored by the base union function.
    If columns do not match, non-matching columns are added with null values to the base dataframes.

    Parameters
    ----------
    session : Session
        session object
    union_dfs : list
        A list of the input snowpark dataframes to union

    Returns
    -------
    snowpark.DataFrame
        A snowpark dataframe with the unioned input dataframes
    
    Example
    -------
        | >> schema_1 = StructType([StructField("a", IntegerType()), StructField("b", StringType())])  
        | >> schema_2 = StructType([StructField("a", FloatType()), StructField("c", StringType())])  
        | >> schema_3 = StructType([StructField("a", IntegerType()), StructField("c", StringType())])  
        | >> schema_4 = StructType([StructField("c", StringType()), StructField("d", StringType())])  

        | >> df_1 = session.create_dataframe([[1, "snow"], [3, "flake"]], schema_1)  
        | >> df_2 = session.create_dataframe([[2.0, "ice"], [4.0, "pick"]], schema_2)  
        | >> df_3 = session.create_dataframe([[6, "test_1"], [7, "test_2"]], schema_3)  
        | >> df_4 = session.create_dataframe([["testing_d", "testing_f"], ["testing_g", "testing_h"]], schema_4)  

        | >> union_dfs = [df_1, df_2, df_3, df_4]  
        | >> unioned_df = auto_union(session, union_dfs)  
        | >> unioned_df.show()  
        | ----------------------------------------  
        | |"A"   |"B"    |"C"        |"D"        |  
        | ----------------------------------------  
        | |1.0   |snow   |NULL       |NULL       |  
        | |3.0   |flake  |NULL       |NULL       |  
        | |2.0   |NULL   |ice        |NULL       |  
        | |4.0   |NULL   |pick       |NULL       |  
        | |6.0   |NULL   |test_1     |NULL       |  
        | |7.0   |NULL   |test_2     |NULL       |  
        | |NULL  |NULL   |testing_d  |testing_f  |  
        | |NULL  |NULL   |testing_g  |testing_h  |  
        | ----------------------------------------  
    
    """
    
    struct_field_list = _get_schemas(union_dfs)
    
    unique_structs_list = _get_unique_struct_fields(struct_field_list)
    
    unique_structs_type_list = _handle_struct_name_type_mismatch(unique_structs_list)
    
    union_df = _create_union_dataframe(session, unique_structs_type_list)
    
    ext_dfs = _add_null_cols(union_dfs, union_df)

    union_df = _populate_union_df(ext_dfs, union_df)
    
    return union_df




def melt():

    return



def isna():

    return


def isnull():

    return


def pivot():

    return


def get_dummies():

    return
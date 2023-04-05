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

from ice_pick.utils import snowpark_query




@dataclass
class SchemaObject:
    # Schema Objects: Table, View, Stream, Stored Proc, File Format, UDF, etc..
    database: str = "SNOWFLAKE"
    schema: str = ""
    object_name: str = ""
    object_type: str = ""

    
    # Which functions should be a part of the class, and 
    # which should be outside teh class?
    def get_ddl(self, session) -> str:
        
        ddl_sql = f"""select get_ddl('{self.object_type}',
                '{self.database}.{self.schema}.{self.object_name}' );"""
        ddl_df = snowpark_query(session, ddl_sql)
        
        ddl_str = ddl_df.iloc[0][0]
        
        return ddl_str


    def get_description(self, session) -> str:
        # (show description sql)\
        desc_sql = f"""describe {self.object_type} 
                    "{self.database}"."{self.schema}"."{self.object_name}";"""
        desc_df = snowpark_query(session, desc_sql, non_select=True)
        
        return desc_df

    
    def get_grants_on(self, session) -> list:
        # return list of objects
        grants_sql = f"""show grants on {self.object_type} 
                    "{self.database}"."{self.schema}"."{self.object_name}";"""
        grants_df = snowpark_query(session, grants_sql, non_select=True)
        
        return grants_df
    
    
    def grant(self, privilege:list, grantee:str, session) -> str:
        # grant access on object, return status

        # -- For TABLE
        #   { SELECT | INSERT | UPDATE | DELETE | TRUNCATE | REFERENCES } [ , ... ]
        # -- For VIEW
        #   { SELECT | REFERENCES } [ , ... ]
        # -- For MATERIALIZED VIEW
        #   { SELECT | REFERENCES } [ , ... ]
        # -- For SEQUENCE, FUNCTION (UDF or external function), PROCEDURE, or FILE FORMAT
        #     USAGE
        # -- For internal STAGE
        #     READ [ , WRITE ]
        # -- For external STAGE
        #     USAGE
        # -- For PIPE
        #    { MONITOR | OPERATE } [ , ... ]
        # -- For STREAM
        #     SELECT
        # -- For TASK
        #    { MONITOR | OPERATE } [ , ... ]
        # -- For MASKING POLICY
        #     APPLY
        # -- For PASSWORD POLICY
        #      APPLY
        # -- For ROW ACCESS POLICY
        #     APPLY
        # -- For SESSION POLICY
        #     APPLY
        # -- For TAG
        #     APPLY
        # -- For ALERT
        #     OPERATE
        # -- For SECRET
        #     USAGE
        
        grant_sql = f"""grant {", ".join(privilege)} on {self.object_type} 
                    "{self.database}"."{self.schema}"."{self.object_name}"
                    to ROLE {grantee};"""
        grant_df = snowpark_query(session, grant_sql, non_select=True)
        
        grant_status_str = grant_df.iloc[0][0]
        
        return grant_status_str
    
    def create(self, session, sql_ext:str = ""):
        # create in snowflake if not exists
        # this is very dependant on objec type
        # usualy the additional stuff comes after the object name
        # sql ext could just make more confusing - maybe need to create specific extension objects
        
        create_sql = f""" create {self.object_type} if not exists
                       "{self.database}"."{self.schema}"."{self.object_name}"
                       {sql_ext}; """
        
        create_df = snowpark_query(session, create_sql, non_select=True)
        
        create_status_str = create_df.iloc[0][0]
        
        return create_status_str
    







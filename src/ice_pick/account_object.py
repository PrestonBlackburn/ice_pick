from dataclasses import dataclass, field
from typing import List
import copy
import re
import configparser

from snowflake.snowpark import Session
import snowflake.snowpark as snowpark
from ice_pick.utils import snowpark_query

import pandas as pd


# Account Objects
# DATABASE
# INTEGRATION
# NETWORK POLICY
# SHARE
# USER
# WAREHOUSE
# SCHEMA
# ROLE
# SHARES
# GRANTS
# RESOURCE MONITOR

class AccountObject:
    def __init__(self, session, name:str, object_type:str):
        self.session = session
        self.name = name
        self.object_type = object_type


    def get_description(self):
        """
        Return the description of the object as a string

        Supports:
        - DATABASE
        - SCHEMA
        - INTEGRATION
        - NETWORK POLICY
        - SHARE
        - USER
        - WAREHOUSE

        """
        support_list = ["database", "schema", "integration", "network policy", "share", "user", "warehouse"]

        if self.object_type not in support_list:
            desc_df = f"""selected warehouse size not supported: 
                         supported warehouse sizes: {support_list}"""
            
            raise ValueError(desc_df)
        
        desc_sql = f"""describe {self.object_type} 
                    "{self.name}";"""
        desc_df = snowpark_query(self.session, desc_sql, non_select=True)

        return desc_df



    def get_ddl(self):
        """
        Return the ddl of the schema object as a string

        Supports:
        - DATABASE
        - SCHEMA

        """
        support_list = ["database", "schema"]

        if self.name not in support_list:
            desc_df = f"""selected warehouse size not supported: 
                         supported warehouse sizes: {support_list}"""
            
            raise ValueError(desc_df)

        
        ddl_sql = f"""select get_ddl('{self.object_type}',
                '{self.name}' );"""
        ddl_df = snowpark_query(self.session, ddl_sql)
        
        ddl_str = ddl_df.iloc[0][0]

        # load to state to help create object (might use later)
        self.ddl_str = ddl_str
        
        return ddl_str




    def get_grants_on(self):
        """
        Supports:

        """
        grants_sql = f""" show grants on {self.object_type} "{self.name}" """

        grants_df = snowpark_query(self.session, grants_sql, non_select=True)
        
        return grants_df



    def drop(self):
        """
        drops object

        Supports:
        most objects
        """

        drop_sql = f""" drop {self.object_type} if exists "{self.name}" """
        
        drop_df = snowpark_query(self.session, drop_sql, non_select=True)

        return drop_df
    


    def un_drop(self):
        """
        undrops a dropped object
        Supports
        - database
        - schema

        """

        undrop_sql = f""" undrop {self.object_type} "{self.name}" """
        
        undrop_df = snowpark_query(self.session, undrop_sql, non_select=True)

        return undrop_df
    


    def create(self, replace:bool = False):
        """
        Supports:

        """
        replace_str = ""

        if replace:
            replace_str = "or replace"

        create_sql = f""" create {replace_str} {self.object_type} "{self.name}" """
        
        create_df = snowpark_query(self.session, create_sql, non_select=True)

        return create_df
    









class Role(AccountObject):
    def __init__(self, session, name:str):
        super().__init__(session, name, "role")

    def show_grants_of(self):

        return

    def show_grants_to(self):

        show_grants_sql = f""" show grants to role {self.name}"""


        return
    
    def show_future_grants(self):

        show_grants_sql = f""" show future grants to role {self.name}"""


        return   








class Database(AccountObject):
    def __init__(self, session, name:str):
        super().__init__(session, name, "database")







class Schema(AccountObject):
    def __init__(self, session, name:str):
        super().__init__(session, name, "schema")







class Integration(AccountObject):
    def __init__(self, session, name:str):
        super().__init__(session, name, "integration")







class User(AccountObject):
    def __init__(self, session, name:str):
        super().__init__(session, name, "user")


    def show_grants_to(self):

        show_grants_sql = f""" show grants to role {self.name}"""

        grants_to_df = snowpark_query(self.session, show_grants_sql, non_select=True)

        return grants_to_df
    

    def check_privilege(self):
        """
        maybe a quick way to show privilege on object?

        """

        return
    

    
    def show_grants_recursive(self):
        """
        If a role is granted to another role, 
        this function will also look at the privileges of the granted roles

        """

        return







class Warehouse(AccountObject):
    def __init__(self, session, name:str):
        super().__init__(session, name, "warehouse")


    def suspend(self):

        suspend_sql  = f""" alter warehouse if exists {self.name} suspend"""
        suspend_df = snowpark_query(self.session, suspend_sql, non_select=True)
        suspend_str = suspend_df.iloc[0][0]

        return suspend_str
    

    def resume(self):

        resume_sql  = f""" alter warehouse if exists {self.name} resume"""
        resume_df = snowpark_query(self.session, resume_sql, non_select=True)
        resume_str = resume_df.iloc[0][0]

        return resume_str
    

    def load_history(self, date_range_start:int, date_range_end:int, interval:str = "hour") -> pd.DataFrame:
        """
        This function returns warehouse activity within the last 14 days.
        This funciton requires elevated privileges to run, either:
        | - The ACCOUNTADMIN role can get results from this function as it has all of the global account permissions.
        | - A role with the MONITOR USAGE global privilege on the ACCOUNT can query this function for any warehouses in the account.
        | - A role with the MONITOR privilege on the WAREHOUSE can query this function for the warehouse it has permissions on.
        | - A role with the OWNERSHIP privilege on the WAREHOUSE has all permissions on the warehouse including MONITOR.
        | (https://docs.snowflake.com/en/sql-reference/functions/warehouse_load_history)
        
        Parameters
        ----------
        date_range_start : int
            start of hours/days ago interval
        date_range_end : int
            end of hours/days ago interval 
        interval: str
            set the date range to either "days" or "hours"

        Returns
        -------
        snowpark.DataFrame
            A snowpark dataframe with the unioned input dataframes
        
        Example
        -------
        Get the load history for the warehouse for the last 12 hours

        | >> warehouse.load_history(12, 0, interval = "hour")
        |
        | Returns Pandas DataFrame With:   
        | - START_TIME
        | - END_TIME
        | - WAREHOUSE_NAME
        | - AVG_RUNNING
        | - AVG_QUEUED_LOAD
        | - AVG_QUEUED_PROVISIONING
        | - AVG_BLOCKED

        """

        
        wh_load_sql = f""" select * from 
                           table(information_schema.WAREHOUSE_LOAD_HISTORY(DATE_RANGE_START => dateadd('{interval}', -{date_range_start}, current_date()),
                                                  DATE_RANGE_END => dateadd('{interval}', -{date_range_end}, current_date()),
                                                  WAREHOUSE_NAME => '{self.name}'))"""
        
        load_hist_df = snowpark_query(self.session, wh_load_sql)
        
        return load_hist_df
    


    def metering_history(self, date_range_start:int, date_range_end:int, interval:str = "hour"):
        """
        This table function can be used in queries to return the hourly credit usage for a single warehouse (or all the warehouses in your account) within a specified date range.  
        This funciton requires elevated privileges to run, either:
        | - The ACCOUNTADMIN role can get results from this function as it has all of the global account permissions.
        | - A role with the MONITOR USAGE global privilege on the ACCOUNT can query this function for any warehouses in the account.
        (https://docs.snowflake.com/en/sql-reference/functions/warehouse_metering_history)  
        
        Parameters
        ----------
        date_range_start : int
            start of hours/days ago interval
        date_range_end : int
            end of hours/days ago interval 
        interval: str
            set the date range to either "days" or "hours"

        Returns
        -------
        snowpark.DataFrame
            A snowpark dataframe with the unioned input dataframes
        
        Example
        -------
        Get the load history for the warehouse for the last 12 hours

        | >> warehouse.metering_history(12, 0, interval = "hour")
        |
        | Returns Pandas DataFrame With:   
        | - START_TIME
        | - END_TIME
        | - WAREHOUSE_NAME
        | - CREDITS_USED
        | - CREDITS_USED_COMPUTE
        | - CREDITS_USED_CLOUD_SERVICES
        
        """
        wh_meter_sql = f""" select * from 
                           table(information_schema.WAREHOUSE_METERING_HISTORY(DATE_RANGE_START => dateadd('{interval}', -{date_range_start}, current_date()),
                                                  DATE_RANGE_END => dateadd('{interval}', -{date_range_end}, current_date()),
                                                  WAREHOUSE_NAME => '{self.name}'))"""
        
        meter_hist_df = snowpark_query(self.session, wh_meter_sql)

        return meter_hist_df
    

    def credits_and_usage_history():
        # TODO
        # need to do some time matching

        return
    

    
    def resize(self, wh_size:str):
        """
        Resize the warehouse to specified size
        Warehouse sizes:
        - XSMALL , 'X-SMALL'
        - SMALL
        - MEDIUM
        - LARGE
        - XLARGE , 'X-LARGE'
        - XXLARGE , X2LARGE , '2X-LARGE'
        - XXXLARGE , X3LARGE , '3X-LARGE'
        - X4LARGE , '4X-LARGE'
        - X5LARGE , '5X-LARGE'
        - X6LARGE , '6X-LARGE'

        Parameters
        ----------
        wh_size : str
            specified warehouse size

        Returns
        -------
        str
            a string with execution status
        
        Example
        -------
        Resize the warehouse to "SMALL" size

        | >> warehouse.resize("SMALL")

        """
        wh_size_list = ["XSMALL",
                         "SMALL",
                         "MEDIUM",
                         "LARGE",
                         "XLARGE",
                         "2XLARGE",
                         "XXLARGE",
                         "3XLARGE",
                         "XXXLARGE",
                         "4XLARGE",
                         "5XLARGE",
                         "6XLARGE"]

        if wh_size.replace("-","").upper() in wh_size_list:
            resize_sql = f""" alter warehouse if exists {self.name}
                            set WAREHOUSE_SIZE = {wh_size}
            """
            resize_df = snowpark_query(self.session, resize_sql, non_select=True)
            resize_str = resize_df.iloc[0][0]
        else:
            resize_str = f"""selected warehouse size not supported: 
                         supported warehouse sizes: {wh_size_list}"""
            
            raise ValueError(resize_str)
        
        
        return resize_str



    def set_auto_suspend(self, seconds:int):
        """
        Specifies the number of seconds of inactivity after which a warehouse is automatically suspended.
        | - Setting a value less than 60 is allowed, but may not result in the desired/expected behavior because the background process that suspends a warehouse runs approximately every 60 seconds and, therefore, is not intended for enabling exact control over warehouse suspension.
        | - Setting a 0 or NULL value means the warehouse never suspends
        
        Parameters
        ----------
        seconds : int
            the number of seconds of inactivity after which a warehouse is automatically suspended

        Returns
        -------
        str
            a string with execution status
        
        Example
        -------
        Set the warehouse timeout to 60 seconds

        | >> warehouse.auto_suspend(60)
        """

        suspend_sql = f""" alter warehouse if exists {self.name}
                            set AUTO_SUSPEND = {seconds}
            """
        suspend_df = snowpark_query(self.session, suspend_sql, non_select=True)
        suspend_str = suspend_df.iloc[0][0]

        return suspend_str


    



    









class NetworkPolicy(AccountObject):
    def __init__(self, session, name:str):
        super().__init__(session, name, "schema")




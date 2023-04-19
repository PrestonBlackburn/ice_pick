from dataclasses import dataclass, field
from typing import List
import copy
import re
import configparser

from snowflake.snowpark import Session
import snowflake.snowpark as snowpark
from ice_pick.utils import snowpark_query
from ice_pick.schema_object import SchemaObject

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

# might remove this class, it seems like a lot of account objects have their own rules
class AccountObject:
    def __init__(self, session, name: str, object_type: str):
        self.session = session
        self.name = name
        self.object_type = object_type

    def __repr__(self):
        return (
            f"{self.__class__.__name__}"
            f"(session={self.session!r}, name={self.name!r}, object_type={self.object_type!r})"
        )

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
        support_list = [
            "database",
            "schema",
            "integration",
            "network policy",
            "share",
            "user",
            "warehouse",
        ]

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

    def create(self, replace: bool = False):
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
    def __init__(self, session, name: str):
        super().__init__(session, name, "role")

    def show_grants_of(self):

        return

    def show_grants_to(self):
        show_grants_sql = f""" show grants to role {self.name}"""
        
        grants_to_df = snowpark_query(self.session, show_grants_sql, non_select=True)
        
        return grants_to_df


    def show_future_grants(self):
        show_grants_sql = f""" show future grants to role {self.name}"""

        return
    

    def show_grants_recursive(self):
        """
        If a role is granted to another role,
        this function will also look at the privileges of the granted roles

        """
        show_grants_sql = f""" show grants to role {self.name}"""

        grants_to_df = snowpark_query(self.session, show_grants_sql, non_select=True)



        return


class User(AccountObject):
    def __init__(self, session, name: str):
        super().__init__(session, name, "user")


    def show_grants_to(self) -> pd.DataFrame:
        """
        return dataframe with grant information
        """
        show_grants_sql = f""" show grants to user {self.name}"""

        grants_to_df = snowpark_query(self.session, show_grants_sql, non_select=True)

        return grants_to_df
    

    def get_roles(self) -> list:
        """
        return granted roles as Role objects
        """

        show_grants_sql = f""" show grants to user {self.name}"""

        grants_to_df = snowpark_query(self.session, show_grants_sql, non_select=True)

        role_name_list = grants_to_df['role'].unique().tolist()

        role_objs = [Role(self.session, role_name) for role_name in role_name_list]
        
        return role_objs
    
    def get_all_privileges(self) -> pd.DataFrame:
        """
        return all privileges for a user
        
        """

        show_grants_sql = f""" show grants to user PRESTONT4"""

        grants_to_user_df = snowpark_query(self.session, show_grants_sql, non_select=True)

        user_role_list = grants_to_user_df['role'].unique().tolist()

        user_role_obj_list = [self.session.Role(user_role) for user_role in user_role_list]

        user_role_dfs = [ user_role_obj.show_grants_to() for user_role_obj in user_role_obj_list]

        comb_user_roles_df = pd.concat(user_role_dfs)

        role_list = comb_user_roles_df[comb_user_roles_df['granted_on'] == 'ROLE']['name'].unique().tolist()

        
        all_privileges_dfs = []
        while(role_list):
            role_obj_list = [self.session.Role(role) for role in role_list]
            
            role_dfs = [role_obj.show_grants_to() for role_obj in role_obj_list]
            
            comb_role_dfs = pd.concat(role_dfs)

            all_privileges_dfs.append(comb_role_dfs)
            
            role_grants_df = comb_role_dfs[comb_role_dfs['granted_on'] == 'ROLE']
            role_list = role_grants_df['name'].unique().tolist()
            

        all_privileges_df = pd.concat(all_privileges_dfs)

        # add back in initial privileges:
        all_privileges_df = pd.concat([all_privileges_df, comb_user_roles_df])
        
        return all_privileges_df
    



    def check_privilege(self, schema_object:SchemaObject) -> list:
        """
        maybe a quick way to show privilege on object?

        """
        all_privileges_df = self.get_all_privileges()

        obj_privileges_df = all_privileges_df[all_privileges_df['name'] == f"{schema_object.database}.{schema_object.schema}.{schema_object.object_name}"]

        obj_privileges_df

        obj_privileges = obj_privileges_df["privilege"].tolist()

        return obj_privileges
    



class Warehouse(AccountObject):
    def __init__(self, session, name: str):
        super().__init__(session, name, "warehouse")

    def suspend(self):
        suspend_sql = f""" alter warehouse if exists {self.name} suspend"""
        suspend_df = snowpark_query(self.session, suspend_sql, non_select=True)
        suspend_str = suspend_df.iloc[0][0]

        return suspend_str

    def resume(self):
        resume_sql = f""" alter warehouse if exists {self.name} resume"""
        resume_df = snowpark_query(self.session, resume_sql, non_select=True)
        resume_str = resume_df.iloc[0][0]

        return resume_str

    def load_history(
        self, date_range_start: int, date_range_end: int, interval: str = "hour"
    ) -> pd.DataFrame:
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

    def metering_history(
        self, date_range_start: int, date_range_end: int, interval: str = "hour"
    ):
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
    


    def query_history(
            self, date_range_start: int, date_range_end: int, interval: str = "hour", result_limit:int = 1000
        ):
        wh_query_hist = f"""select * from 
                            table (information_schema.QUERY_HISTORY_BY_WAREHOUSE(
                                WAREHOUSE_NAME => '{self.name}', 
                                END_TIME_RANGE_START => dateadd('hours',-{date_range_start},current_timestamp()),
                                END_TIME_RANGE_END => dateadd('hours',{date_range_end},current_timestamp()),
                                RESULT_LIMIT => {result_limit} )
                                );"""
        meter_hist_df = snowpark_query(self.session, wh_query_hist)

        return meter_hist_df
    


    def resize_recommendation(self, auto_apply:bool = False) -> str:
        """
        Keeping this simple as a starting point.
        In most cases rules should be customized for individual use cases.
        Rules:
        | - If: Local disk spillage over the last 3 days > 30% for the longest running queries
        | - Then: recommend size up warehouse
        |
        | - If: Local disk spillage over the last 3 days < 2% for the longest running queries
        | - Then: recommend downsizing warehouse
        |
        | - If: Max queued overload time > 10 minutes
        | - And: Local disk spillage over the last 3 days < 2% for the longest running queries
        | - Then: recommend scaling out
        |
        | - If: Max queued overload time > 10 minutes
        | - And: Local disk spillage over the last 3 days > 2% for the longest running queries
        | - Then: recommend scaling up

        """
        query_df = self.query_history(36, 0, interval = "hour", result_limit = 1000)

        # filter out queries that don't use warehouse compute
        query_df = query_df[pd.notna(query_df['WAREHOUSE_SIZE'])]

        top_5_queries_df = query_df.nlargest(5, 'EXECUTION_TIME')

        top_5_query_list = top_5_queries_df['QUERY_ID'].tolist()

        # combine querys so we just make one request

        execution_queries = []
        for query in top_5_query_list:
            execution_sql = f"""select 
            EXECUTION_TIME_BREAKDOWN:local_disk_io as local_disk,
            EXECUTION_TIME_BREAKDOWN:remote_disk_io as remote_disk,
            EXECUTION_TIME_BREAKDOWN:processing as processing
                from table(get_query_operator_stats('{query}'))
            """
            execution_queries.append(execution_sql)

        execution_queries_sql = " union ".join(execution_queries)

        top_query_df = snowpark_query(self.session, execution_queries_sql)

        mean_local_disk_pct = top_query_df[pd.notna(top_query_df['LOCAL_DISK'])]['LOCAL_DISK'].astype(float).mean() * 100
        
        # mean overload time of top 5 longest queued time queries
        mean_queued_overload_time = query_df['QUEUED_OVERLOAD_TIME'].nlargest(5).mean()


        wh_recommendation = "No Change"

        if mean_local_disk_pct > 30:
            wh_recommendation = "Size Up"
        if mean_local_disk_pct < 2:
            wh_recommendation = "Size Down"
        if mean_queued_overload_time > 10 and mean_local_disk_pct < 5:
            wh_recommendation = "Scale Out"
        if mean_queued_overload_time > 10 and mean_local_disk_pct >= 5:
            wh_recommendation = "Size Up"


        if auto_apply:
            # TODO
            pass

        return wh_recommendation
    


    def resize(self, wh_size: str):
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
        wh_size_list = [
            "XSMALL",
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
            "6XLARGE",
        ]

        if wh_size.replace("-", "").upper() in wh_size_list:
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

    def set_auto_suspend(self, seconds: int):
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





# ---------------    In Progress    ------------------
class Database(AccountObject):
    def __init__(self, session, name: str):
        super().__init__(session, name, "database")


class Schema(AccountObject):
    def __init__(self, session, name: str):
        super().__init__(session, name, "schema")


class Integration(AccountObject):
    def __init__(self, session, name: str):
        super().__init__(session, name, "integration")

class NetworkPolicy(AccountObject):
    def __init__(self, session, name: str):
        super().__init__(session, name, "schema")


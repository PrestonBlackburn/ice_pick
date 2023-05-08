
from dataclasses import dataclass, field
from typing import List, Union, Literal
import copy
import re
import configparser

from snowflake.snowpark import Session
import snowflake.snowpark as snowpark
from ice_pick.utils import snowpark_query
from ice_pick.schema_object import SchemaObject
from ice_pick.account_object import (
    Account,
    AccountObject,
    Role,
    User,
    Warehouse,
    Database,
    Schema,
    Integration,
    NetworkPolicy,
    ResourceMonitor,
)

import pandas as pd




# -----------------------------     supported privileges    -------------------------------

def get_supported_privileges():
    """
        A helper method to define the avaiable privileges for the assocated Snowflake object.

    """
    global_privileges = {
                        Account: [
                        'CREATE ACCOUNT',
                        'CREATE DATA EXCHANGE',
                        'CREATE LISTING',
                        'CREATE DATABASE',
                        'CREATE INTEGRATION',
                        'CREATE NETWORK POLICY',
                        'CREATE ROLE',
                        'CREATE SHARE',
                        'CREATE USER',
                        'CREATE WAREHOUSE',
                        'APPLY MASKING POLICY',
                        'APPLY PASSWORD POLICY',
                        'APPLY ROW ACCESS POLICY',
                        'APPLY SESSION POLICY',
                        'APPLY TAG',
                        'ATTACH POLICY',
                        'EXECUTE ALERT',
                        'EXECUTE TASK',
                        'IMPORT SHARE',
                        'MANAGE GRANTS',
                        'MONITOR EXECUTION',
                        'MONITOR USAGE',
                        'OVERRIDE SHARE RESTRICTIONS',
                        'RESOLVE ALL'
                    ]
                }   

    account_object_privileges = {
        User: ['MONITOR', 'OWNERSHIP'],
        Role: ['USAGE', 'OWNERSHIP'],
        ResourceMonitor: ['MODIFY', 'MONITOR', 'OWNERSHIP'],
        Warehouse: ['MODIFY', 'MONITOR', 'USAGE', 'OPERATE', 'OWNERSHIP'],
        Database: ['CREATE DATABASE ROLE', 'CREATE SCHEMA', 'IMPORTED PRIVILEGES', 'MODIFY', 'MONTIOR', 'USAGE', 'OWNERSHIP'],
        Integration: ['USAGE', 'USE_ANY_ROLE', 'OWNERSHIP'],
        Schema: ['MODIFY', 
                'MONITOR',
                'USAGE',
                'CREATE ALERT',
                'CREATE EXTERNAL TABLE',
                'CREATE FILE FORMAT',
                'CREATE FUNCTION',
                'CREATE MASKING POLICY',
                'CREATE MATERIALIZED VIEW',
                'CREATE PASSWORD POLICY',
                'CREATE PIPE',
                'CREATE PROCEDURE',
                'CREATE ROW ACCESS POLICY',
                'CREATE SECRET',
                'CREATE SESSION POLICY',
                'CREATE SEQUENCE',
                'CREATE STAGE',
                'CREATE STREAM',
                'CREATE TAG',
                'CREATE TABLE',
                'CREATE TASK',
                'CREATE VIEW',
                'ADD SEARCH OPTIMIZATION',
                'OWNERSHIP'
            ]

        }


    schema_object_privileges = {
        "TABLE": ['SELECT', 'INSERT', 'UPDATE', 'TRUNCATE', 'REFERENCES', 'OWNERSHIP'],
        "VIEW": ['SELECT', 'REFERENCES', 'OWNERSHIP'],
        "MATERIALIZED VIEW": ['SELECT', 'REFERENCES', 'OWNERSHIP'],
        "SEQUENCE": ["USAGE", 'OWNERSHIP'],
        "USER FUNCTION": ["USAGE", 'OWNERSHIP'],
        "EXTERNAL FUNCTION": ["USAGE", 'OWNERSHIP'],
        "PROCEDURE": ["USAGE", 'OWNERSHIP'],
        "FILE FORMAT": ["USAGE", 'OWNERSHIP'],
        "INTERNAL STAGE": ['READ', 'WRITE', 'OWNERSHIP'],
        "EXTERNAL STAGE": ['USAGE', 'OWNERSHIP'],
        "PIPE": ['MONITOR', 'OPERATE', 'OWNERSHIP'],
        "MASKING POLICY": ['APPLY', 'OWNERSHIP'],
        "PASSWORD POLICY": ['APPLY', 'OWNERSHIP'],
        "ROW ACCESS POLICY": ['APPLY', 'OWNERSHIP'],
        "SESSION POLICY": ['APPLY', 'OWNERSHIP'],
        "TAG": ['APPLY', 'OWNERSHIP'],
        "ALERT": ['OPERATE', 'OWNERSHIP'],
        "SECRET": ['USAGE', 'OWNERSHIP'],
    }

    return global_privileges, account_object_privileges, schema_object_privileges





@dataclass
class Privilege:
    """
        can pass any schema object or account object
        Mostly here just to help validation
    """

    """Represents a Privilege In Snowflake

    can pass any schema object, account object, or account paired with desired privilege

    Attributes
    ----------
    object: Union[SchemaObject, AccountObject, Account]
        The object that the privilege is on
    privilege: str
        the privilege on the object


    Example
    ------------


    """
    object: Union[SchemaObject, AccountObject, Account]
    definition:str


    # privilege input validation:
    def __post_init__(self):
        global_privileges, account_object_privileges, schema_object_privileges = get_supported_privileges()
        self.privilege = self.definition.upper()

        # Get privilege options and make sure they match
        if isinstance(self.object, AccountObject):
            privilege_options = account_object_privileges[self.object.__class__]

            if self.definition not in privilege_options:
                raise ValueError(f"""privilege {self.definition} is not in privilege options {privilege_options} 
                                  for account object type {self.object}""")

        elif isinstance(self.object, SchemaObject):
            privilege_options = schema_object_privileges[self.object.object_type]

            if self.definition not in privilege_options:
                raise ValueError(f"""privilege {self.definition} is not in privilege options {privilege_options} 
                                  for schema object type {self.object.object_type}""")
            
        elif isinstance(self.object, Account):
            privilege_options = global_privileges[self.object.__class__]

            if self.definition not in privilege_options:
                raise ValueError(f"""privilege {self.definition} is not in privilege options {privilege_options} 
                                  for account {self.object.object_type}""")
            
        else:
            raise ValueError(f'The input object {self.object} is not an Account, AccountObject or SchemaObject')






class Grant:
    def __init__(self,
                session: Session, 
                privilege: Privilege,
                role: Role,
                grant_option: str = None,
                future_str: str = None):
        
        self.session = session
        self.privilege_str = privilege.definition
        self.role = role
        self.on_object = privilege.object
        self.grant_option = grant_option
        self.future_str = future_str


    def __repr__(self):
        return (
            f"{self.__class__.__name__}"
            f"(session={self.session!r}, role={self.role!r}, on_object={self.on_object!r}, privilege_str={self.privilege_str!r})"
        )
    

    def check_exists(self) -> bool:

        return
    


    def execute_grant(self):
        """ create the grant """

        grant_sql = f""" grant {self.privilege_str} on {self.on_object} to {self.role.name} """

        grant_df = snowpark_query(self.session, grant_sql, non_select=True)

        return grant_df


    def revoke():

        return
    
    
    def validate(self, GrantTree):
        # to check precident/dependant grants

        return
    


from dataclasses import dataclass, field
from typing import List, Dict
import copy
import re
import configparser
import warnings
import logging

from snowflake.snowpark import Session
import snowflake.snowpark as snowpark
from snowflake.snowpark.row import Row

import pandas as pd
import numpy as np

from ice_pick.utils import snowpark_query
from ice_pick.schema_object import SchemaObject
from ice_pick.account_object import AccountObject
from ice_pick.account_object import (
    AccountObject,
    Warehouse, 
    Role, 
    User,
    Database,
    Schema,
    Integration,
    NetworkPolicy,
    ResourceMonitor,
)




def _default_field(obj):
    return field(default_factory=lambda: copy.deepcopy(obj))


@dataclass
class SchemaObjectFilter:
    """
    A filter that can be used to return multiple SchemaObjects

    Apply selection first, then filter out ingore objects.

    Filters are applied by:
        database -> ignore_dbs -> schema -> ignore_schemas -> object type -> object name

    ".*" can be used to return all (regex supported)

    Attributes
    ----------
    session: Session
        Snowpark Session
    databases: list
        databases that wil be searched
    schemas: list
        schemas that wil be searched
    object_names: list
        the name of the objects to be searched for
    object_types: list
        the type of schema objects to be searched for
    ignore_dbs: list
        databases to be ignored in search
    ignore_schemas: list
        schemas to be ignored in search



    """

    session: Session
    databases: list  # ex: [".*"]
    schemas: list  # ex: ["schema_*"]
    object_names: list  # ex: [".*"]
    object_types: list  # ex: [".*"]

    ignore_dbs: list = _default_field(
        ["SNOWFLAKE_SAMPLE_DATA", "SNOWFLAKE"]
    )  # ex: ("SNOWFLAKE", "SNOWFLAKE_*")
    ignore_schemas: list = _default_field(
        ["INFORMATION_SCHEMA"]
    )  # ex: ("SNOWFLAKE.*") # requires fully specified name

    def _filter_schema_objects_helper(
        self,
        objects_df: pd.DataFrame,
        filtered_dbs: str,
        filtered_schemas: str,
        obj_type: str,
    ) -> pd.DataFrame:
        """
        a helper function for filtering dataframe for objects
        """
        filtered_dbs_str = "|".join(filtered_dbs)
        obj_filtered_db_df = objects_df[
            objects_df["database_name"].str.contains(filtered_dbs_str)
        ]

        filtered_schemas_str = "|".join(filtered_schemas)
        obj_filtered_schema_df = obj_filtered_db_df[
            obj_filtered_db_df["schema_name"].str.contains(filtered_schemas_str)
        ]

        obj_filtered_schema_df["object_type"] = obj_type.rsplit("S", 1)[0]

        return obj_filtered_schema_df

    def _filter_schema_objects(
        self, filtered_dbs: str, filtered_schemas: str
    ) -> pd.DataFrame:
        """
        helper function to get all schema level object info
         - get the object types that are selected
         - get the object info (database, schema, object type, object name)
         - The object info returned depends on the selected object types (see schema_level_exceptions)


        """

        # (almost) all schema level objects:
        schema_level_objects = [
            "ALERTS",
            "EXTERNAL FUNCTIONS",
            "EXTERNAL TABLES",
            "FILE FORMATS",
            "MATERIALIZED VIEWS",
            "MASKING POLICIES",
            "PASSWORD POLICIES",
            "PIPES",
            "PROCEDURES",
            "ROW ACCESS POLICIES",
            "SECRETS",
            "SESSION POLICIES",
            "SEQUENCES",
            "STAGES",
            "STREAMS",
            "TABLES",
            "TAGS",
            "TASKS",
            "USER FUNCTIONS",
            "VIEWS",
        ]

        # Schema level objects with some exceptions
        schema_level_exceptions = ["EXTERNAL FUNCTIONS", "PROCEDURES", "USER FUNCTIONS"]

        # get the objects to execute "show" on:
        obj_type_filter = []
        for type_obj in self.object_types:
            # to upper or not to upper..
            type_obj = type_obj.upper()
            r = re.compile(type_obj)
            obj_list = list(filter(r.match, schema_level_objects))
            obj_type_filter.extend(obj_list)

        # loop through object types for "show" query
        obj_df_list = []
        for obj_type in obj_type_filter:
            try:
                objects_sql = f""" show {obj_type} in account; """
                objects_df = snowpark_query(self.session, objects_sql, non_select=True)
            except Exception as e:
                logging.warning("Some objects require a Enterprise or Buisness Critial account to access")
                objects_df = pd.DataFrame()
            # g4t dfs to format: name, schema_name, database_name
            # need to filter based on name and type:
            if objects_df.empty:
                warnings.warn(f"No objects found for: {obj_type}", UserWarning)
                continue

            if obj_type not in schema_level_exceptions:
                objects_df = objects_df[["database_name", "schema_name", "name"]]

                obj_filtered_schema_df = self._filter_schema_objects_helper(
                    objects_df, filtered_dbs, filtered_schemas, obj_type
                )

                obj_df_list.append(obj_filtered_schema_df)

            else:
                # handle the exceptions
                # In these cases database_name = catalog_name
                # Also, we need the input arguments with the name, but not the output arguments
                func_df = objects_df[["catalog_name", "schema_name", "arguments"]]
                func_df = func_df.rename(
                    {"catalog_name": "database_name", "arguments": "name"}, axis=1
                )
                # Get just the function name + input arguments
                func_df["name"] = func_df["name"].apply(
                    lambda x: x.split(" RETURN ")[0]
                )

                # now we can filter down:
                obj_filtered_schema_df = self._filter_schema_objects_helper(
                    func_df, filtered_dbs, filtered_schemas, obj_type
                )

                obj_df_list.append(obj_filtered_schema_df)

        if obj_df_list == []:
            warnings.warn(
                f"""No objects found with filter:
                            databases: {self.databases}
                            schemas: {self.schemas}
                            object_types: {self.object_types}
                            object_names: {self.object_names}
                            ingore_dbs: {self.ignore_dbs} 
                            ignore_schemas: {self.ignore_schemas}""",
                UserWarning,
            )
            return []

        else:
            all_objs_df = pd.concat(obj_df_list)

            return all_objs_df

    def return_schema_objects(self) -> List[SchemaObject]:
        """
        Filter objects based on input objects
        If the property is a wildcard ".*", then search all objects at that level
        (inputs are passed as regex)

        If exclude is set to true, everything matched will be ignored, and all non-matches are returned

        Parameters
        ----------
        None :


        Returns
        -------
        List[SchemaObjects]
            a list of schema objects that matched the filter cases

        Example
        -------
        | Get all procedures in all databases:
        | >> SchemaObjectFilter([".*"], [".*"], [".*"], ["procedure"])

        | Get all tables and vies in a single database:
        | >> SchemaObjectFilter(["TEST_DB"], [".*"], [".*"], ["table", "view"])

        | Get all tables except for the sample tables:
        | >> SchemaObjectFilter([".*"], [".*"],[".*"], ["table"], ingore_dbs = ["SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"]

        | Get specific tables:
        | >> SchemaObjectFilter(["snowflake"], ["sample_data"], ["customer", "transactions"], ["table"])

        """

        # create select / ignore strings for filtering
        db_select_str = "|".join(self.databases)
        db_ignore_str = "|".join(self.ignore_dbs)

        schema_select_str = "|".join(self.schemas)
        schema_ignore_str = "|".join(self.ignore_schemas)

        object_select_str = "|".join(self.object_names)
        type_select_str = "|".join(self.object_types)

        # Return Databases based on filters
        dbs_sql = f""" show databases in account; """
        dbs_df = snowpark_query(self.session, dbs_sql, non_select=True)

        db_select_filter_df = dbs_df[dbs_df["name"].str.contains(db_select_str)]
        db_ignore_filter_df = db_select_filter_df[
            ~db_select_filter_df["name"].str.contains(db_ignore_str)
        ]

        logging.debug(f"Database level dataframe after filtering: \n {db_ignore_filter_df}")

        filtered_dbs = db_ignore_filter_df["name"].tolist()

        # Return Schemas based on filters
        schemas_sql = f""" show schemas in account; """
        schemas_df = snowpark_query(self.session, schemas_sql, non_select=True)

        filtered_dbs_str = "|".join(filtered_dbs)
        db_filtered_schemas_df = schemas_df[
            schemas_df["database_name"].str.contains(filtered_dbs_str)
        ]
        
        schema_select_filter_df = db_filtered_schemas_df[
            db_filtered_schemas_df["name"].str.contains(schema_select_str)
        ]
        # Ignore the databases selected previously
        schema_ignore_filter_df = schema_select_filter_df[
            ~schema_select_filter_df["database_name"].str.contains(db_ignore_str)
        ]   
        # ignore the schemas from the schema ignore string     
        schema_ignore_filter_df = schema_ignore_filter_df[
            ~schema_select_filter_df["name"].str.contains(schema_ignore_str)
        ]


        logging.debug(f"Schema level databaframe after filtering: \n {schema_ignore_filter_df}")
        filtered_schemas = schema_ignore_filter_df["name"].tolist()

        # Return Objects
        all_objs_df = self._filter_schema_objects(filtered_dbs, filtered_schemas)

        # Construct the SchemaObject:
        #    database, schema, object_name, object_type
        logging.debug(f" Dataframe of all of the objects returned after the filter: \n {all_objs_df}")

        # return all_objs_df

        # handle the case when there are no objects:
        if isinstance(all_objs_df, list):
            return []

        # otherwise create schema objects for the dataframe
        schema_object_series = all_objs_df.apply(
            lambda x: SchemaObject(
                self.session,
                x["database_name"],
                x["schema_name"],
                x["name"],
                x["object_type"],
            ),
            axis=1,
        )

        schema_object_list = schema_object_series.tolist()

        return schema_object_list









@dataclass
class AccountObjectFilter:
    """
    A filter that can be used to return multiple AccountObjects

    Apply selection first, then filter out ingore objects.

    Filters are applied by:
        object_types -> object_names -> ignore_names

    ".*" can be used to return all (regex supported)

    Attributes
    ----------
    session: Session
        Snowpark Session
    object_names: list
        objects that will be searched
    object_types: list
        object types that will be searched
    ignore_names: list
        names to be ignored in search
    """

    session: Session
    object_names: list  # ex: [".*"]
    object_types: list  # ex: [".*"]
    ignore_names: list =  _default_field(
        ["example_of_name_to_ignore"]
    )


    def _query_account_object_helper(self) -> Dict[str, pd.DataFrame]:
        """ 
            get all avialable account objects based on type
            Return a dictionary of the object type and the assocated pandas dataframe
            (todo - compare vs just getting everything then filtering)
        """

        account_level_objects = [
            "WAREHOUSES",
            "ROLES",
            "USERS",
            "DATABASES",
            "SCHEMAS",
            "INTEGRATIONS",
            "NETWORK POLICIES", 
            "RESOURCE MONITORS",
        ]

        account_level_exceptions = ["INTEGRATIONS", "NETWORK POLICIES"]

        # get selected account object types
        # selected_account_obj_types =       
        selected_account_level_objects = []      
        for obj_type in self.object_types:
            
            obj_type = obj_type.upper()
            r = re.compile(obj_type)
            selected_objects = list(filter(r.match, account_level_objects))

            # handle the case of network policy
            if "NETWORK" in obj_type and obj_type not in "NETWORK POLICIES":
                selected_objects.extend(["NETWORK POLICIES"])

            selected_account_level_objects.extend(selected_objects)

        logging.debug(f"selected account level objects after text preprocessing: {selected_account_level_objects}")

        account_object_collection = {}

        for obj in selected_account_level_objects:

            # get dataframe for objects without anything weird in the "show" statement
            if obj not in account_level_exceptions:
                try:
                    obj_info_sql = f""" show {obj} in account"""
                    objects_df = snowpark_query(self.session, obj_info_sql, non_select=True)
                    logging.debug(f"initial objects in account for {obj}: {objects_df}")
                except Exception as e:
                    logging.warning("Some objects require a Enterprise or Buisness Critial account to access")
                    objects_df = pd.DataFrame()

                # remove trailing "S":
                updated_obj_type = ""
                for i, char in enumerate(obj):
                    if i == (len(obj)-1) and char == "S":
                        updated_obj_type += ""
                    else:
                        updated_obj_type += char

                
                account_object_collection.update({updated_obj_type: objects_df})

            # get dataframe for all other objects with "show" statement:
            # - Integrations
            # - Network policies
            if obj in account_level_exceptions:
                if obj == "INTEGRATIONS":
                    try:
                        integration_types = ["API INTEGRATIONS",
                                              "SECURITY INTEGRATIONS", 
                                              "NOTIFICATION INTEGRATIONS",
                                              "STORAGE INTEGRATIONS"]
                        integration_sql_list = [ f""" show {obj}""" for integ_type in integration_types ]
                        integration_dfs = [snowpark_query(self.session,
                                                           integ_sql, non_select=True) for integ_sql in integration_sql_list]
                        integration_df = pd.concat(integration_dfs)

                    except Exception as e:
                        logging.warning("Some objects require a Enterprise or Buisness Critial account to access")
                        integration_df = pd.DataFrame()

                    account_object_collection.update({"INTEGRATION": integration_df})

                if obj == "NETWORK POLICIES":
                    try:
                        network_policy_sql = f""" show NETWORK POLICIES in account"""
                        network_policy_df = snowpark_query(self.session, network_policy_sql, non_select=True)
                    except Exception as e:
                        logging.warning("Some objects require a Enterprise or Buisness Critial account to access")
                        network_policy_df = pd.DataFrame()
                    
                    account_object_collection.update({"NETWORK POLICY": network_policy_df})

        return account_object_collection
    

    def _filter_name_account_objects(self, account_object_collection: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """ Filter objects to only selected names """

        filtered_account_object_collection = {}
        for key, value_df in account_object_collection.items():

            if value_df is None:
                continue
            if value_df.empty:
                continue

            filtered_name_str = "|".join(self.object_names)
            filtered_df = value_df[
                value_df["name"].str.contains(filtered_name_str)
            ]
            filtered_account_object_collection.update({key: filtered_df})

        return filtered_account_object_collection
    

    def _filter_ignore_account_objects(self, account_object_collection: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """ Filter objects to remove "ignore" names """

        filtered_account_object_collection = {}
        
        for key, value_df in account_object_collection.items():

            if value_df is None:
                continue
            if value_df.empty:
                continue

            if self.ignore_names:

                filtered_name_str = "|".join(self.ignore_names)
                filtered_df = value_df[
                    ~value_df["name"].str.contains(filtered_name_str)
                ]
                filtered_account_object_collection.update({key: filtered_df})
            
            else:
                # just copy if no ignore names
                filtered_account_object_collection.update({key: value_df})

        return filtered_account_object_collection


    def __create_account_object_instances(self, object_type:str, object_names:list) -> List[AccountObject]:
        """ create a single account object helper for creating all account objects"""

        account_objects_map = {    
            "WAREHOUSE": Warehouse, 
            "ROLE": Role,
            "USER": User,
            "DATABASE": Database,
            "SCHEMA": Schema,
            "INTEGRATION": Integration,
            "NETWORK POLIYC": NetworkPolicy,
            "RESOURCE MONITOR": ResourceMonitor,
        }

        account_object_instances = []
        for name in object_names:
            AccountObjectClassType = account_objects_map[object_type]
            account_object_instance = AccountObjectClassType(self.session, name)

            account_object_instances.append(account_object_instance)

        return account_object_instances


    def _create_account_objects(self, account_object_collection:Dict[str, pd.DataFrame]) -> List[AccountObject]:
        """ create the objects frm the account object collection """


        # for now just getting "name" information to create the classes
        object_names_collection = {}
        for key, value_df in account_object_collection.items():

            if value_df is None:
                continue
            if value_df.empty:
                continue

            account_obj_names = value_df['name'].tolist()
            object_names_collection.update({key: account_obj_names})
        

        all_account_obj_instances = []
        for key, name_list in object_names_collection.items():
            account_object_instances = self.__create_account_object_instances(key, name_list)
            all_account_obj_instances.extend(account_object_instances)
        
        return all_account_obj_instances



    def return_account_objects(self) -> List[AccountObject]:
        """
        Return all account objects matching the filter
        """
        account_object_collection = self._query_account_object_helper()
        logging.debug(f"account collection after initial retrieval: \n {account_object_collection}")
        
        account_object_collection_name_filter = self._filter_name_account_objects(account_object_collection)
        logging.debug(f"account collection after name filtering: \n {account_object_collection_name_filter}")
        
        account_object_collection_complete_filter = self._filter_ignore_account_objects(account_object_collection_name_filter)
        logging.debug(f"account collection after all filtering: \n {account_object_collection_complete_filter}")
        
        account_object_instances = self._create_account_objects(account_object_collection_complete_filter)

        return account_object_instances




@dataclass
class PrivilegeFilter:
    session: Session





@dataclass 
class GrantFilter:
    session: Session


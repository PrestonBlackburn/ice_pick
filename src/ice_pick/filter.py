from dataclasses import dataclass, field
from typing import List
import copy
import re
import configparser
import warnings

from snowflake.snowpark import Session
import snowflake.snowpark as snowpark
from snowflake.snowpark.row import Row

import pandas as pd
import numpy as np

from ice_pick.utils import snowpark_query
from ice_pick.schema_object import SchemaObject


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
            objects_sql = f""" show {obj_type} in account; """
            objects_df = snowpark_query(self.session, objects_sql, non_select=True)

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

        # Return Databases
        dbs_sql = f""" show databases in account; """
        dbs_df = snowpark_query(self.session, dbs_sql, non_select=True)

        db_select_filter_df = dbs_df[dbs_df["name"].str.contains(db_select_str)]
        db_ignore_filter_df = db_select_filter_df[
            ~db_select_filter_df["name"].str.contains(db_ignore_str)
        ]
        filtered_dbs = db_ignore_filter_df["name"].tolist()

        # Return Schemas
        schemas_sql = f""" show schemas in account; """
        schemas_df = snowpark_query(self.session, schemas_sql, non_select=True)

        filtered_dbs_str = "|".join(filtered_dbs)
        db_filtered_schemas_df = schemas_df[
            schemas_df["database_name"].str.contains(filtered_dbs_str)
        ]

        schema_select_filter_df = db_filtered_schemas_df[
            db_filtered_schemas_df["name"].str.contains(schema_select_str)
        ]
        schema_ignore_filter_df = schema_select_filter_df[
            ~schema_select_filter_df["name"].str.contains(schema_ignore_str)
        ]
        filtered_schemas = schema_ignore_filter_df["name"].tolist()

        # Return Objects
        all_objs_df = self._filter_schema_objects(filtered_dbs, filtered_schemas)

        # Construct the SchemaObject:
        #    database, schema, object_name, object_type
        # print(all_objs_df)

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
    session: Session
    object_names: list  # ex: [".*"]
    object_types: list  # ex: [".*"]

    def return_account_objects():
        """
        Return all account objects matching the filter
        """
        return

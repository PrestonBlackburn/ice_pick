from dataclasses import dataclass, field
from typing import List
import copy
import re
from pathlib import Path


from snowflake.snowpark import Session
import snowflake.snowpark as snowpark
from snowflake.snowpark.row import Row

import pandas as pd
import numpy as np

from ice_pick.utils import snowpark_query


@dataclass
class SchemaObject:
    """Represents a Snowflake Schema object.

    Schema Objects Include: ALERTS, EXTERNAL FUNCTIONS, EXTERNAL TABLES, FILE FORMATS,
    MATERIALIZED VIEWS, MASKING POLICIES, PASSWORD POLICIES, PIPES, PROCEDURES,
    ROW ACCESS POLICIES, SECRETS, SESSION POLICIES, SEQUENCES, STAGES, STREAMS,
    TABLES, TAGS, TASKS, USER FUNCTIONS,  VIEWS, *EXTERNAL FUNCTIONS,
     *PROCEDURES, *USER FUNCTIONS
     ** Also note some of these objects require the enterprise account

    Attributes
    ----------
    session: Session
        Snowpark Session
    database: str
        database that the object is in
    schema: str
        schema that the object is in
    object_name: str
        the name of the object
    object_type: str
        the type of schema object


    """

    # Schema Objects: Table, View, Stream, Stored Proc, File Format, UDF, etc..
    session: Session
    database: str = "SNOWFLAKE"
    schema: str = ""
    object_name: str = ""
    object_type: str = ""

    # Which functions should be a part of the class, and
    # which should be outside teh class?
    def get_ddl(self, save: bool = False) -> str:
        """
        Return the ddl of the schema object as a string
        if save = True: save the ddl locally
        The default save path is:
        DDL/database/schema/object_type/database.schema.object_name.sql

        Parameters
        ----------
        save : bool = False
            save the ddl as a file locally

        Returns
        -------
        str
            A string with the ddl

        """
        ddl_exception_list = ["USER FUNCTION", "USER POLICY"]
        ddl_exception_map = {"USER FUNCTION": "FUNCTION", "USER POLICY": "POLICY"}

        if self.object_type in ddl_exception_list:
            self.ddl_object_type = ddl_exception_map[self.object_type]
        else:
            self.ddl_object_type = self.object_type

        ddl_sql = f"""select get_ddl('{self.ddl_object_type}',
                '{self.database}.{self.schema}.{self.object_name}' );"""
        ddl_df = snowpark_query(self.session, ddl_sql)

        ddl_str = ddl_df.iloc[0][0]

        # load to state to help create object
        self.ddl_str = ddl_str

        if save:
            path = f"DDL/{self.database}/{self.schema}/{self.object_type}"
            filename = f"{self.database}.{self.schema}.{self.object_name}.sql"
            output_file = Path(f"{path}/{filename}")
            output_file.parent.mkdir(exist_ok=True, parents=True)
            output_file.write_text(ddl_str)

        return ddl_str

    def get_description(self) -> str:
        """
        Return the description of the schema object as a string
        """
        desc_sql = f"""describe {self.object_type} 
                    "{self.database}"."{self.schema}"."{self.object_name}";"""
        desc_df = snowpark_query(self.session, desc_sql, non_select=True)

        return desc_df

    def get_grants_on(self) -> list:
        """
        Return a list of grants on the schema object as a list
        """
        grants_sql = f"""show grants on {self.object_type} 
                    "{self.database}"."{self.schema}"."{self.object_name}";"""
        grants_df = snowpark_query(self.session, grants_sql, non_select=True)

        return grants_df

    def grant(self, privilege: list, grantee: str) -> str:
        """grant access on object, return status

        | -- For TABLE
        |   { SELECT | INSERT | UPDATE | DELETE | TRUNCATE | REFERENCES } [ , ... ]
        | -- For VIEW
        |   { SELECT | REFERENCES } [ , ... ]
        | -- For MATERIALIZED VIEW
        |   { SELECT | REFERENCES } [ , ... ]
        | -- For SEQUENCE, FUNCTION (UDF or external function), PROCEDURE, or FILE FORMAT
        |     USAGE
        | -- For internal STAGE
        |     READ [ , WRITE ]
        | -- For external STAGE
        |     USAGE
        | -- For PIPE
        |    { MONITOR | OPERATE } [ , ... ]
        | -- For STREAM
        |     SELECT
        | -- For TASK
        |    { MONITOR | OPERATE } [ , ... ]
        | -- For MASKING POLICY
        |     APPLY
        | -- For PASSWORD POLICY
        |      APPLY
        | -- For ROW ACCESS POLICY
        |     APPLY
        | -- For SESSION POLICY
        |     APPLY
        | -- For TAG
        |     APPLY
        | -- For ALERT
        |     OPERATE
        | -- For SECRET
        |     USAGE

        """

        grant_sql = f"""grant {", ".join(privilege)} on {self.object_type} 
                    "{self.database}"."{self.schema}"."{self.object_name}"
                    to ROLE {grantee};"""
        grant_df = snowpark_query(self.session, grant_sql, non_select=True)

        grant_status_str = grant_df.iloc[0][0]

        return grant_status_str

    def create(
        self,
        create_method: str = "default",
        ddl: str = "",
        sql_ext: str = "",
        create_if_exists: bool = False,
    ):
        """create in snowflake if not exists. For now this is very dependant on object type.
        Usualy the additional stuff comes after the object name, which can be provided by the sql_ext param.
        (todo: sql ext could just make more confusing - maybe need to create specific extension objects)

        create_methods:
            - default:
            - ddl:        user provided ddl string
            - ddl_state:  use ddl from get_ddl() function
        """

        if create_method == "default":
            create_sql = f""" create {self.object_type} if not exists
                        "{self.database}"."{self.schema}"."{self.object_name}"
                        {sql_ext}; """

        if create_method == "ddl_state":
            create_sql = self.ddl_str

        if create_method == "ddl":
            create_sql = ddl

        create_df = snowpark_query(self.session, create_sql, non_select=True)

        create_status_str = create_df.iloc[0][0]

        return create_status_str

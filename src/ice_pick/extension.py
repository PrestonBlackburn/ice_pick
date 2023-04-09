"""
The extensions module monkey patches the Snowpark Session to add additional functionality.
For now this is to give ice_pick a more native feel, where added ice_pick functions can be called from the Snowpark Session class.
"""

from snowflake.snowpark import Session

from ice_pick.schema_object import SchemaObject
from ice_pick.filter import SchemaObjectFilter
from ice_pick.utils import auto_union_standalone


#todo - create a wrapper to help with monkey patching

def create_schema_object(self, database, schema, object_name, object_type):

    return SchemaObject(self, database, schema, object_name, object_type)



def create_schema_object_filter(self, databases, schemas, object_names,
                                object_types, ignore_dbs = ["SNOWFLAKE_SAMPLE_DATA", "SNOWFLAKE"],
                                ignore_schemas = ["INFORMATION_SCHEMA"]):

    return SchemaObjectFilter(self, databases, schemas, object_names,
                                object_types, ignore_dbs,
                                ignore_schemas)


def auto_union(self, union_dfs:list):

    unioned_dfs = auto_union_standalone(self, union_dfs)

    return unioned_dfs



def extend_session(Session: Session) -> Session:
    """
    Returns the extended Session class

    Parameters
    ----------
    session : Session
        Snowpark Session

    Returns
    -------
    Session
        The exteneded Snowpark Session
    
    Example
    -------
    >> session = extend_session(Session).builder.configs(connection_parameters).create()
    """
    
    # adding the methods to create the schema objects
    Session.create_schema_object = create_schema_object 
    Session.create_schema_object_filter = create_schema_object_filter
    Session.auto_union = auto_union

    return Session


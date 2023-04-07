
from snowflake.snowpark import Session

from ice_pick.schema_object import SchemaObject
from ice_pick.filter import SchemaObjectFilter



def create_schema_object(self, database, schema, object_name, object_type):

    return SchemaObject(self, database, schema, object_name, object_type)



def create_schema_object_filter(self, databases, schemas, object_names,
                                object_types, ignore_dbs = ["SNOWFLAKE_SAMPLE_DATA", "SNOWFLAKE"],
                                ignore_schemas = ["INFORMATION_SCHEMA"]):

    return SchemaObjectFilter(self, databases, schemas, object_names,
                                object_types, ignore_dbs,
                                ignore_schemas)




def extend_session(Session: Session) -> Session:
    """
    Returns the extended Session class
    """
    
    # adding the methods to create the schema objects
    Session.create_schema_object = create_schema_object 
    Session.create_schema_object_filter = create_schema_object_filter

    return Session


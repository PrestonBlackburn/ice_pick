"""
The extensions module monkey patches the Snowpark Session to add additional functionality.
For now this is to give ice_pick a more native feel, where added ice_pick functions can be called from the Snowpark Session class.
"""

from snowflake.snowpark import Session

from ice_pick.schema_object import SchemaObject
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

from ice_pick.filters import SchemaObjectFilter, AccountObjectFilter
from ice_pick.utils import concat_standalone
from ice_pick.utils import melt_standalone

from ice_pick.account_object import AccountObject, Warehouse, Role, User


# todo - create a wrapper/decorator to help with monkey patching
# (can probably do most of this at the function level instead of importing everyting)



# -----------------------  Schema Level Extensions ----------------------------

def create_schema_object(self, database, schema, object_name, object_type):
    return SchemaObject(self, database, schema, object_name, object_type)

def create_schema_object_filter(
    self,
    databases,
    schemas,
    object_names,
    object_types,
    ignore_dbs=["SNOWFLAKE_SAMPLE_DATA", "SNOWFLAKE"],
    ignore_schemas=["INFORMATION_SCHEMA"],
):
    return SchemaObjectFilter(
        self, databases, schemas, object_names, object_types, ignore_dbs, ignore_schemas
    )





# -----------------------  Account Level Extensions ----------------------------

def create_account_object(self, name, object_type):
    return AccountObject(self, name, object_type)

def create_warehouse(self, name):
    return Warehouse(self, name)

def create_role(self, name):
    return Role(self, name)

def create_user(self, name):
    return User(self, name)

def create_database(self, name):
    return Database(self, name)

def create_schema(self, name):
    return Schema(self, name)

def create_integration(self, name):
    return Integration(self, name)

def create_network_policy(self, name):
    return NetworkPolicy(self, name)

def create_resource_monitor(self, name):
    return ResourceMonitor(self, name)





def create_account_object_filter(
       self,
       object_names,
       object_types,
       ignore_names = None,
    
):
    return AccountObjectFilter(
        self, object_names, object_types, ignore_names
    )






# ---------------------  Pandas like utils --------------------------

def concat(self, union_dfs: list):
    unioned_dfs = concat_standalone(self, union_dfs)

    return unioned_dfs


def melt(
    self,
    df,
    id_vars: list,
    value_vars: list,
    var_name: str = "variable",
    value_name: str = "value",
):
    melt_df = melt_standalone(
        self, df, id_vars, value_vars, var_name=var_name, value_name=value_name
    )

    return melt_df






# -----------------------  Extend the session with the new functionality  ---------------------------

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

    # adding the methods to create the account objects
    Session.create_account_object = create_account_object
    Session.create_role = create_role
    Session.create_warehouse = create_warehouse
    Session.Ucreate_user = create_user
    Session.Dcreate_database = create_database
    Session.create_schema = create_schema
    Session.create_integration = create_integration
    Session.create_network_policy = create_network_policy
    Session.create_resource_monitor = create_resource_monitor

    Session.create_account_object_filter = create_account_object_filter
    

    # adding the methods to create misc functions
    Session.concat = concat
    Session.melt = melt

    return Session

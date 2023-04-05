# Ice Pick

## What is it?

**Ice Pick** is a Python package that provides utilities for common operations done on a Snowflake warehouse. Operations range from getting Snowflake object ddl, getting table statistics, and returning account level information.

Many utilities can be automatically packaged and deployed as stored procedures, so they can be executed natively in Snowflake.


## Main Features

Interact with schema objects with a Snowpark like interface.  
In this initial development two data classes are available: `SchemaObject` and `SchemaObjectFilter`. The `SchemaObject` allows you to interact and retrieve information on the schema object, such as ddl, description, and grants.  
<br/>
The `SchemaObjectFilter` allows you to quickly return and inspect many schema objects. With the `SchemaObjectFilter` you can filter on specific databases, schemas, objects or object types using regex. Alternatively you can specify ignore filters to return all objects not in the filter.  
<br/>


## Getting Started
(before pushed to pypi)

### install the library
```bash
python -m pip install -e .
```

### create a snowpark session
```python
from snowflake.snowpark import Session

connection_parameters = {
  "account": "<your snowflake account>",
  "user": "<your snowflake user>",
  "password": "<your snowflake password>",
  "role": "<snowflake user role>",
  "warehouse": "<snowflake warehouse>",
  "database": "<snowflake database>",
  "schema": "<snowflake schema>"
}

session = Session.builder.configs(connection_parameters).create()
```
## Example Usage
### Return the ddl, description, and grants for a schema object 
```python
from ice_pick import SchemaObject

obj = SchemaObject('TEST', 'SCHEMA_1', 'CUSTOMER', 'TABLE')

# Get the ddl for the specified object
ddl = obj.get_ddl(session)

# Get the description of the object
description = obj.get_description(session)

# Get the grants on the object
grants_on = obj.get_grants_on(session)

# Grant the object to a role with permissions specified in a list
grant = obj.grant(["SELECT"], "PUBLIC", session)
```


### Return bulk ddl for tables and procedures in the TEST database and the databases matching the TEST_* pattern 
```python
from ice_pick import SchemaObject, SchemaObjectFilter

# create the fitler, where "*" are wildcards (regex supported)
obj_filter = SchemaObjectFilter(
                ["TEST", "TEST_*"],        # databases
                [".*"],                    # schemas
                [".*"],                    # object names
                ["tables", "Procedures"]   # object types
            )

# return the schema objects based on the filter
schema_object_list = sp_filter.return_schema_objects(session)

# Get the ddl for the returned schema objects
ddl_list = [schema_obj.get_ddl(session) for schema_obj in schema_object_list]
```


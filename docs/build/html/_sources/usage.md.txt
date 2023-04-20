# Usage

## Installation

to install snowflake_ice_pick
```console
(.venv) $ pip install snowflake_ice_pick
```

## Setup
Ice pick extends the Snowpark `Session`, so before using Ice Pick a Snowpark session must be created and extended
```python
from ice_pick import extend_session
from snowflake.snowpark import Session

connection_parameters = {
"account": "<your snowflake account>",
"user": "<your snowflake user>",
"password": "<your snowflake password>",
"role": "<your snowflake role>",  # optional
"warehouse": "<your snowflake warehouse>",  # optional
"database": "<your snowflake database>",  # optional
"schema": "<your snowflake schema>",  # optional
}  

session = extend_session(Session).builder.configs(connection_parameters).create()  
```

Once the session is extended you are able to use additional objects and functions in Snowpark. These include two main classes of objects: **Account Objects** (ex: Warehouses, Users, Roles, etc...)  and **Schema Objects** (ex: Tables, Views, Procedures, Pipes, etc...).  
References to these new classes of objects can be done using the extended session.  

```python
# Account objects can be created with a familiar syntax to Snowpark dataframes or tables
# example creating a Snowpark/ice pick User object
user = session.User("PRESTONT4")

# Format for creating a schema object: 
# session.create_schema_object(database, schema, object name, object type)

# example of creating a table object
customer_table = session.create_schema_object('TEST', 'SCHEMA_1', 'CUSTOMER', 'TABLE')
```



```{eval-rst}
.. autofunction:: src.ice_pick.connection.connector.get_credentials
```

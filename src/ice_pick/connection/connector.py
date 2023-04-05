import snowflake.connector
import configparser


# Key pair auth
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization


# haven't decided if I should support this...

# connection methods:
# Maybe - config file, env vars
def get_credentials(conf_path:str, section:str = 'DEFAULT') -> dict:
    """
    Getting credentials with configparser

    Parameters
    ------------
    conf_path
        A string for the config path

    section
        The section of the .config file to use

    """
    config = configparser.ConfigParser()
    config.read(conf_path)
    conf_dict = dict(config[section])

    return conf_dict



def create_snowflake_connector(conf_dict:dict) -> snowflake.connector:
    """
    Initializing the connection to Snowflake

    Parameters
    ------------
    conf_dict
        a dictionary of the connection vairables to be passed to the snowflake connector
        
    """

    conn = snowflake.connector.connect(**conf_dict)

    return conn

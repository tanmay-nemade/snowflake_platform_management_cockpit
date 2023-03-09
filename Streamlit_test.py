import os
import configparser
from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
import pandas as pd
import streamlit as st

st.set_page_config(
    layout="wide",
    page_title="Snowflake Platform Management Cockpit"
)

st.title('Snowflake Platform Management Cockpit')

config = configparser.ConfigParser()
ini_path = os.path.join(os.getcwd(),'config.ini')
config.read(ini_path)
sfAccount = config['SnowflakePOC']['sfAccount']
sfUser = config['SnowflakePOC']['sfUser']
sfPass = config['SnowflakePOC']['sfPass']
sfRole = config['SnowflakePOC']['sfRole']
sfDB = config['SnowflakePOC']['sfDB']
sfSchema = config['SnowflakePOC']['sfSchema']
sfWarehouse = config['SnowflakePOC']['sfWarehouse']

# conn = {                            "account": st.secrets["sfAccount"],
#                                     "user": st.secrets["sfUser"],
#                                     "password": st.secrets["sfPass"],
#                                     "role": st.secrets["sfRole"],
#                                     "warehouse": st.secrets["sfWarehouse"]}
#                                     #"database": sfDB,
#                                     #"schema": sfSchema}

conn = {                            "account": sfAccount,
                                    "user": sfUser,
                                    "password": sfPass,
                                    "role": sfRole,
                                    "warehouse": sfWarehouse}
                                    #"database": sfDB,
                                    #"schema": sfSchema}

# # Step 3 Create a session using the connection parameters
session = Session.builder.configs(conn).create()

dataframe1 = session.sql('select * from garden_plants.veggies.root_depth; ').collect()
st.table(dataframe1)
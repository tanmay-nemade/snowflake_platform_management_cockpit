import os
import configparser
from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
import pandas as pd
import streamlit as st

# # Page config must be set
#comment
st.set_page_config(
    layout="wide",
    page_title="File Upload App"
)

st.title('Auto Object Creation Utility')

# # Step 2 Create connection parameters
#Configure config.ini file path
config = configparser.ConfigParser()
config.read('config.ini')
sections = config.sections()
accounts = []
for section in sections:
    accounts.append(section)
    

#Function to select the Snowflake Account
def sfAccount_selector(account):
    #setup config.ini read rules
    sfAccount = config[account]['sfAccount']
    sfUser = config[account]['sfUser']
    sfPass = config[account]['sfPass']
    sfRole = config[account]['sfRole']
    #sfDB = config[account]['sfDB']
    #sfSchema = config[account]['sfSchema']
    sfWarehouse = config[account]['sfWarehouse']

    #dictionary with names and values of connection parameters
    conn = {"account": sfAccount,
            "user": sfUser,
            "password": sfPass,
            "role": sfRole,
            "warehouse": sfWarehouse}
            #"database": sfDB,
            #"schema": sfSchema}

    #Create a session using the connection parameters
    session = Session.builder.configs(conn).create()
    return session


def db_list(session):
    dbs = session.sql("show databases ;").collect()
    #db_list = dbs.filter(col('name') != 'SNOWFLAKE')
    db_list = [list(row.asDict().values())[1] for row in dbs]
    return db_list


def schemas_list(chosen_db, session):
    # .table() tells us which table we want to select
    # col() refers to a column
    # .select() allows us to chose which column(s) we want
    # .filter() allows us to filter on coniditions
    # .distinct() means removing duplicates
    
    session.sql('use database :chosen_db;')
    fq_schema_name = chosen_db+'.information_schema.tables'
    

    schemas = session.table(fq_schema_name)\
            .select(col("table_schema"),col("table_catalog"),col("table_type"))\
            .filter(col('table_schema') != 'INFORMATION_SCHEMA')\
            .filter(col('table_type') == 'BASE TABLE')\
            .distinct()
            
    schemas_list = schemas.collect()
    # The above function returns a list of row objects
    # The below turns iterates over the list of rows
    # and converts each row into a dict, then a list, and extracts
    # the first value
    schemas_list = [list(row.asDict().values())[0] for row in schemas_list]
    return schemas_list


def file_to_upload(chosen_db, chosen_schema, chosen_table):
    label_for_file_upload = "Select file to ingest into {d}.{s}.{t}"\
      .format(d = chosen_db, s = chosen_schema, t = chosen_table)
    return label_for_file_upload


def table_create(db_select, sc_select,tt, table_name, session, chosen_file, if_replce):
    #table creation part of sql
    if if_replace:
        if tt == 'TRANSIENT':
            create_table_str = 'create or replace transient table ' + db_select + '.'+ sc_select +'.'+ table_name
        elif tt == 'PERMANENT':
            create_table_str = 'create or replace table ' + db_select + '.'+ sc_select +'.'+ table_name
    else:
        if tt == 'TRANSIENT':
            create_table_str = 'create transient table ' + db_select + '.'+ sc_select +'.'+ table_name
        elif tt == 'PERMANENT':
            create_table_str = 'create table ' + db_select + '.'+ sc_select +'.'+ table_name
    #column definition part
    col_def = " ("
    data_set = chosen_file
    cols = len(data_set.columns)
    for i in range(cols):
        col_def = col_def + " " +str(data_set.columns[i]) + " " + str(data_set.iloc[0][i])
        if i != cols - 1:
            col_def = col_def + ","
    col_def = col_def + ");"
    final = session.sql(create_table_str + col_def).collect()
    data_set = data_set.drop(0)
    num_of_rows = len(data_set)
    try:
        try:
            session.sql('use schema ' +db_select+'.'+sc_select).collect()
            session.write_pandas(
                        df=data_set,
                        table_name=table_name,
                        database=db_select,
                        schema=sc_select,
                        overwrite=False,
                        quote_identifiers=False
                    )
            with st.sidebar:
                st.markdown("Your upload was a success. You uploaded {r} rows.".format(r = num_of_rows))
        except Exception as e:
            with st.sidebar:
                st.markdown("Your upload was not succesful. \n " + str(e))
    except:
        with st.sidebar:
                st.markdown("Table already exists. \n ")


acc_select = st.selectbox('Choose account',(accounts))
session = sfAccount_selector(acc_select)
database = db_list(session)
db_select = st.selectbox('Choose Database',(database))
schemas = schemas_list(db_select, session)
sc_select = st.selectbox('Choose Schema',(schemas))
tt=st.selectbox('Table Type',( 'PERMANENT','TRANSIENT'))
table_name = st.text_input('Enter Table Name')
if_replace = st.checkbox('Replace Table if it already exist')

# Step 12 Create a radio input with the schemas_list() function
chosen_file = st.file_uploader(label=file_to_upload(db_select,sc_select,table_name))
if chosen_file is not None:
    data = pd.read_csv(chosen_file)
    Run = st.button("EXECUTE", on_click = table_create,args=[db_select,sc_select,tt,table_name, session, data, if_replace])
if chosen_file is not None:
    st.table(data)


# Step 13 Create a radio input with the schemas_list() function
# print_message = st.write(upload_file(db_select,sc_select, Run, chosen_file, 'Test Table'))

###############################################################################
# Program: File2Table.py
#
# Author: Craig Nobili
#
# Create Date: 06-25-2020
#
# Purpose: Loads Alameda Country Social Services data into table TBL_AHS_SSA
#
# Revisions:
#
###############################################################################

import sys
import datetime
import re
import pandas as pd
import pyodbc
import urllib
from sqlalchemy import create_engine, text

def get_date_time():
  now = datetime.datetime.now()
  #print (now.strftime("%Y-%m-%d %H:%M:%S"))
  return now.strftime("%Y-%m-%d %H:%M:%S") 

def usage(prg_name):

  print()
  print("Usage: " + prg_name + " filename delimiter header loadType schema tablename dbConnStr bulkinsert createTable [columns]")
  print()
  print("  filename    - Full path of file to load")
  print("  delimiter   - Column delimiter in file")
  print("  header      - Indicate (yes or no) whether the file has a header record of columns")
  print("  loadType    - Indicates (truncate or append) the load type")
  print("  schema      - Database schema name")
  print("  tablename   - Database table name")
  print("  dbConnStr   - Database connection string")
  print("  bulkinsert  - Indicates (yes or no) whether to use bulk insert (faster load)")
  print("  createTable - Indicates (yes or no) whehter to create the database table")
  print("  columns     - Option list of comma delimiter columns if, i.e. if no header record is in the file")
  print()
  print("Examples:")
  print(prg_name + ' theFile.csv , yes truncate theSchema theTable "Driver={SQL Server};Server=ahs-bi-db;Database=K2;Trusted_Connection=yes;" yes')
  print(prg_name + ' theFileNoHeader.csv , no append theSchema theTable "Driver={SQL Server};Server=ahs-bi-db;Database=K2;Trusted_Connection=yes;" yes col1,col2,col3')
  print()
  
def file_to_dataframe(file_path, delimiter, header, column_list):

  header_rec = ""
  cols = []
  
  if delimiter == "TAB":
    delim = "\t"
  else:
    delim = delimiter
  
  if header == "yes":
    f = open(file_path, "r")
    header_rec = f.readline().rstrip('\n')
    f.close()
    cols = header_rec.split(delim)
    cols = [re.sub('[^a-zA-Z0-9]+', '', _) for _ in cols]
  else:
    cols = column_list
    
  if header == "yes":
    data = pd.read_csv(file_path, names = cols, header = 0, sep = delim)
  else:
    data = pd.read_csv(file_path, names = cols, header = None, sep = delim)
          
  df = pd.DataFrame(data)
  
  #Add metadata columns
  load_date_time = get_date_time()
  df.insert(loc = len(df.columns), column = "Filename", value = file_path)
  df.insert(loc = len(df.columns), column = "LoadDateTime", value = load_date_time);
                               
  return df   
  
def load_sql_table(conn_str, load_type, bulk_insert, schema_name, table_name, create_table, df):
  #db_con = pyodbc.connect(conn_str)
  #bulk insert may fail when converting strings to dates if missing leading zeros in months or days
  quoted = urllib.parse.quote_plus(conn_str)
  if bulk_insert == "yes":
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted), fast_executemany=True)
  else:
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
  
  if create_table == "yes":
    print("{} -->Create table = {}.{}".format(get_date_time(), schema_name, table_name))
    
    comma = ""
    create_stmt = "create table " + schema_name + "." + table_name + " ("
    for col in df.columns:
      create_stmt += comma + col + " varchar(8000)"
      comma = ", "
    create_stmt += " )"
    
    connection = engine.connect()
    connection.execute(create_stmt)
    connection.close()
    
  if load_type == "truncate":
    print("{} -->truncate table = {}.{}".format(get_date_time(), schema_name, table_name))
    connection = engine.connect()
    truncate_query = text("truncate table " + schema_name + "." + table_name)
    connection.execution_options(autocommit=True).execute(truncate_query)
    connection.close()
    
  df.to_sql(con = engine, schema = schema_name, name = table_name, if_exists = 'append', index = False) 
  print("{} -->Loaded table = {}.{}".format(get_date_time(), schema_name, table_name))
                                                               
def main():

  prg_name = sys.argv[0]
  
  if not(len(sys.argv) == 10 or len(sys.argv) == 11):
    usage(prg_name)
    return
  
  filename     = sys.argv[1]
  delimiter    = sys.argv[2]
  header       = sys.argv[3]
  load_type    = sys.argv[4]
  schema       = sys.argv[5]
  table_name   = sys.argv[6]
  db_conn_str  = sys.argv[7]
  bulk_insert  = sys.argv[8]
  create_table = sys.argv[9]
  
  print()
  print(f"filename     = {filename}")
  print(f"delimiter    = {delimiter}")
  print(f"header       = {header}")
  print(f"load_type    = {load_type}")
  print(f"schema       = {schema}")
  print(f"table_name   = {table_name}")
  print(f"db_conn_str  = {db_conn_str}")
  print(f"bulk_insert  = {bulk_insert}")
  print(f"create_table = {bulk_insert}")
  
  columns = ""
  column_list = []
  if len(sys.argv) == 11:
    columns = sys.argv[10]
    column_list = columns.split(",")
  
  print()
  print("{} -->Load file = {} into DataFrame".format(get_date_time(), filename))
  df = file_to_dataframe(filename, delimiter, header, column_list)
  #print(df)
    
  print("{} -->Load DataFrame into table = {}".format(get_date_time(), table_name))
  load_sql_table(db_conn_str, load_type, bulk_insert, schema, table_name, create_table, df)
    
#
# Program Entry Point
#
if __name__ == "__main__":
    main()

  
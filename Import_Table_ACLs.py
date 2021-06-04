# Databricks notebook source
# MAGIC %md #Import Table ACLs
# MAGIC 
# MAGIC Imports Table ACLS from a JSON file on DBFS, which has been generated by the Export_Table_ACLs script
# MAGIC 
# MAGIC Parameters:
# MAGIC - InputPath: Path to the JSON file to import from (gzipped JSON) 
# MAGIC Returns: (`dbutils.notebook.exit(exit_JSON_string)`)
# MAGIC - `{ "total_num_acls": <int>, "num_sucessfully_executed": <int>, "num_execution_errors": <int>, , "num_error_entries_acls": <int> }` 
# MAGIC   - total_num_acls : ACL entries sucessfully executed
# MAGIC   - num_sucessfully_executed : ACL entries sucessfully executed
# MAGIC   - num_execution_errors : valid ACL entries that caused an error when executing
# MAGIC   - num_error_entries_acls : error entries in the imported JSON ignored, principal is set to `ERROR_!!!` and object_key and object_value are prefixed with `ERROR_!!!`
# MAGIC 
# MAGIC Execution: **Run the notebook on a cluster with Table ACL's enabled as a user who is an admin** 
# MAGIC 
# MAGIC Supported object types:
# MAGIC - Catalog: included if all databases are exported, not included if databases to be exported are specified
# MAGIC   - Database: included
# MAGIC     - Table: included
# MAGIC     - View: (they are treated as tables with ObjectType `TABLE`)
# MAGIC - Anonymous Function: included (testing pending)
# MAGIC - Any File: included
# MAGIC 
# MAGIC Unsupported object types:
# MAGIC - User Function: Currently in Databricks SQL not supported - will add support later
# MAGIC 
# MAGIC JSON File format: Line of JSON objects, gzipped
# MAGIC 
# MAGIC - written as `.coalesce(1).format("JSON").option("compression","gzip")`
# MAGIC - each line contains a JSON object with the keys:
# MAGIC   - `Database`: string
# MAGIC   - `Principal`: string
# MAGIC   - `ActionTypes`: list of action strings: 
# MAGIC   - `ObjectType`: `(ANONYMOUS_FUNCTION|ANY_FILE|CATALOG$|DATABASE|TABLE|ERROR_!!!_<type>)` (view are treated as tables)
# MAGIC   - `ObjectKey`: string
# MAGIC   - `ExportTimestamp`: string
# MAGIC - error lines contains:
# MAGIC   - the special `Principal` `ERROR_!!!` 
# MAGIC   - `ActionTypes` contains one element: the error message, starting with `ERROR!!! :`
# MAGIC   - `Database`, `ObjectType`, `ObjectKey` are all prefixed with `ERROR_!!!_`
# MAGIC - error lines are ignored by the Import_Table_ACLs 
# MAGIC 
# MAGIC The JSON file is written as table, because on a cluster with Table ACLS activated, files cannot be written directly.
# MAGIC The output path will contain other files and diretories, starting with `_`, which can be ignored.
# MAGIC 
# MAGIC 
# MAGIC What to do if exported JSON contains errors (the notebook returns `num_errors` > 0):
# MAGIC - If there are only a few errors ( e.g. less then 1% or less then dozen)
# MAGIC   - proceed with the import (any error lines will be ignored)
# MAGIC   - For each error, the object type, name and error message is included so the cause can be investigated
# MAGIC     - in most cases, it turns out that those are broken or not used tables or views
# MAGIC - If there are many errors
# MAGIC   - Try executing some `SHOW GRANT` commands on the same cluster using the same user, there might be a underlying problem
# MAGIC   - review the errors and investiage

# COMMAND ----------

# DBTITLE 1,Define Parameters
#dbutils.widgets.removeAll()
dbutils.widgets.text("InputPath","dbfs:/tmp/migrate/test_table_acls.json.gz","1: Input Path")

# COMMAND ----------

# DBTITLE 1,Validated Parameters
if not dbutils.widgets.get("InputPath").startswith("dbfs:/"):
   raise Exception(f"Unexpected value for notebook parameter 'InputPath', got <{dbutils.widgets.get('InputPath')}>, but it must start with <dbfs:/........>")

# COMMAND ----------

# DBTITLE 1,Show Input Data
display(spark.read.format("JSON").load(dbutils.widgets.get("InputPath")))

# COMMAND ----------

# DBTITLE 1,Define Import Logic
import datetime
import pyspark.sql.functions as sf
from typing import Callable, Iterator, Union, Optional, List
import sys
import json

def generate_table_acls_command(action_types, object_type, object_key, principal, alter_owner=True):
  lines = []
  
  grant_privs = [ x for x in action_types if not x.startswith("DENIED_") and x != "OWN" ]
  deny_privs = [ x[len("DENIED_"):] for x in action_types if x.startswith("DENIED_") and x != "OWN" ]

  # TODO consider collapsing to all priviledges if all are granted

  if grant_privs:
    lines.append(f"GRANT {', '.join(grant_privs)} ON {object_type} {object_key} TO `{principal}`;")
  if deny_privs:
    lines.append(f"DENY {', '.join(deny_privs)} ON {object_type} {object_key} TO `{principal}`;")
    
  #TODO !!! NOT QUITE SURE WETHER ALTER OWNER ACTUALLY WORKS !!!!!!!!!!!  
  if alter_owner and "OWN" in action_types:
    lines.append(f"ALTER {object_type} {object_key} OWNER TO `{principal}`;")
      
  return lines
   

def generate_table_acls_commands(table_ACLs_df, commented: bool=True, alter_owner: bool=True) -> List[str]:
  lines = []
  total_num_acls = 0
  num_error_entries_acls = 0
  for row in table_ACLs_df.collect():
    
    
    if row["ObjectType"] == "ANONYMOUS_FUNCTION":
      lines.extend(generate_table_acls_command(row['ActionTypes'], 'ANONYMOUS FUNCTION', '', row['Principal'], alter_owner))
    elif row["ObjectType"] == "ANY_FILE":
      lines.extend(generate_table_acls_command(row['ActionTypes'], 'ANY FILE', '', row['Principal'], alter_owner))
    elif row["ObjectType"] == "CATALOG$":
      lines.extend(generate_table_acls_command(row['ActionTypes'], 'CATALOG', '', row['Principal'], alter_owner))
    elif row["ObjectType"] in ["DATABASE", "TABLE"]:
      # DATABASE, TABLE, VIEW (view's seem to show up as tables)
      lines.extend(generate_table_acls_command(row['ActionTypes'], row['ObjectType'], row['ObjectKey'], row['Principal'], alter_owner))
    # TODO ADD   USER FUNCTION .. need to figure out
    
    if row["Principal"] == "ERROR_!!!":
      num_error_entries_acls = num_error_entries_acls + 1
    else
      total_num_acls = total_num_acls + 1
   
      
  return lines, total_num_acls, num_error_entries_acls

def execute_sql_statements(sqls):
  num_sucessfully_executed = 0
  num_execution_errors = 0
  error_causing_sqls = []
  for sql in sqls.split(sep=";"):
    sql = sql.strip()
    if sql:
      print(f"{sql};")
      try:
        spark.sql(sql)
        num_sucessfully_executed = num_sucessfully_executed+1
      except:
        error_causing_sqls.append({'sql': sql, 'error': sys.exc_info()})
        num_execution_errors = num_execution_errors+1
      
  return num_sucessfully_executed, num_execution_errors, error_causing_sqls

      

# COMMAND ----------

# DBTITLE 1,Run Import
input_path = dbutils.widgets.get("InputPath")

  
table_ACLs_df = spark.read.format("JSON").load(input_path).orderBy("Database","ObjectType")

print(f"{datetime.datetime.now()} reading table ACLs from {input_path}")


lines, total_num_acls, num_error_entries_acls = generate_table_acls_commands(table_ACLs_df, commented=True, alter_owner=True)

sql="\n".join(lines)

print(f"Number of table ACLs statements to execute: {len(lines)}")
print("\n\n")

num_sucessfully_executed, num_execution_errors, error_causing_sqls = execute_sql_statements(sql)

#Returns: (`dbutils.notebook.exit(exit_JSON_string)`)
#- `{ "total_num_acls": <int>, "num_sucessfully_executed": <int>, "num_execution_errors": <int>, , "num_error_entries_acls": <int> }` 
#  - total_num_acls : ACL entries sucessfully executed
#  - num_sucessfully_executed : ACL entries sucessfully executed
#  - num_execution_errors : valid ACL entries that caused an error when executing
#  - num_error_entries_acls : error entries in the imported JSON ignored, principal is set to `ERROR_!!!` and object_key and object_value are prefixed with `ERROR_!!!`

exit_JSON_string = f'''{ 
  "total_num_acls":  {total_num_acls}
  ,"num_sucessfully_executed": {num_sucessfully_executed}
  ,"num_execution_errors": {num_execution_errors}
  ,"num_error_entries_acls": {num_error_entries_acls}
}'''


# COMMAND ----------

# DBTITLE 1,Error causing SQL messages and error messages:
if len(num_error_entries_acls) == 0:
  print("No SQL errors")
else:
  print(f"Number of SQL errors: {len(num_error_entries_acls)}\n\n")
  print(json.dumps(num_error_entries_acls, indent=2))

# COMMAND ----------

# DBTITLE 1,Write the notebook exit JSON string value

print(exit_JSON_string)

dbutils.notebook.exit(exit_JSON_string) 

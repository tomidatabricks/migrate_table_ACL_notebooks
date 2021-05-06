# Databricks notebook source
# MAGIC %md #Import Table ACLs
# MAGIC 
# MAGIC Imports Table ACLS from a JSON file on DBFS, which has been generated by the Export_Table_ACLs script
# MAGIC 
# MAGIC Parameters:
# MAGIC - InputPath: Path to the JSON file to import from (gzipped JSON) 
# MAGIC 
# MAGIC Execution: **Run the notebook on a cluster with Table ACL's enabled as a user who is an admin** 
# MAGIC 
# MAGIC Supportes ACLs for Object types:
# MAGIC - Catalog: included if all databases are exported, not included if databases to be exported are specified
# MAGIC   - Database: included
# MAGIC     - Table: included
# MAGIC     - View: included
# MAGIC - Anonymous Function: included (testing pending)
# MAGIC - Any File: included
# MAGIC 
# MAGIC Disclaimer: This notebook is still needs some more testing, check back soon as fixes might have been added.

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
      
  return lines

def execute_sql_statements(sqls):
  for sql in sqls.split(sep=";"):
    sql = sql.strip()
    if sql:
      print(f"{sql};")
      spark.sql(sql)

      

# COMMAND ----------

# DBTITLE 1,Run Import
input_path = dbutils.widgets.get("InputPath")

  
table_ACLs_df = spark.read.format("JSON").load(input_path).orderBy("Database","ObjectType")

print(f"{datetime.datetime.now()} reading table ACLs from {input_path}")


lines = generate_table_acls_commands(table_ACLs_df, commented=True, alter_owner=True)

sql="\n".join(lines)

print(f"Number of table ACLs statements to execute: {len(lines)}")
print("\n\n")

execute_sql_statements(sql)

# COMMAND ----------



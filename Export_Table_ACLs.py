# Databricks notebook source
# MAGIC %md #Export Table ACLs
# MAGIC 
# MAGIC Exports Table ACLS to a JSON file on DBFS, which can be imported using the Import_Table_ACLs script
# MAGIC 
# MAGIC Parameters:
# MAGIC - Databases: [Optional] comma separated list of databases to be exported, if empty all databases will be exported
# MAGIC - OutputPath: Path to write the exported file to 
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

# DBTITLE 1,Declare Parameters
#dbutils.widgets.removeAll()
dbutils.widgets.text("Databases","db_acl_test,db_acl_test_restricted","1: Databases (opt)")
dbutils.widgets.text("OutputPath","dbfs:/tmp/migrate/test_table_acls.json.gz","2: Output Path")

# COMMAND ----------

# DBTITLE 1,Check Parameters
    
if not dbutils.widgets.get("OutputPath").startswith("dbfs:/"):
   raise Exception(f"Unexpected value for notebook parameter 'InputPath', got <{dbutils.widgets.get('OutputPath')}>, but it must start with <dbfs:/........>")


# COMMAND ----------

# DBTITLE 1,Define Export Logic
import pyspark.sql.functions as sf
from typing import Callable, Iterator, Union, Optional, List
import datetime
import sys

def get_database_names():
  database_names = []
  for db in spark.sql("show databases").collect():
    if hasattr(db,"databaseName"): #Angela has this fallback ...
      database_names.append(db.databaseName)
    else:
      database_names.append(db.namespace)
  return database_names

def create_grants_df(database_name: str,object_type: str, object_key: str) -> List[str]:
  try:
    if object_type in ["CATALOG", "ANY FILE", "ANONYMOUS FUNCTION"]: #without object key
       grants_df = (
         spark.sql(f"SHOW GRANT ON {object_type}")
         .groupBy("ObjectType","ObjectKey","Principal").agg(sf.collect_set("ActionType").alias("ActionTypes"))
         .selectExpr("NULL AS Database","Principal","ActionTypes","ObjectType","ObjectKey","Now() AS ExportTimestamp")
       )
    else: 
      grants_df = (
        spark.sql(f"SHOW GRANT ON {object_type} {object_key}")
        .filter(sf.col("ObjectType") == f"{object_type}")
        .groupBy("ObjectType","ObjectKey","Principal").agg(sf.collect_set("ActionType").alias("ActionTypes"))
        .selectExpr(f"'{database_name}' AS Database","Principal","ActionTypes","ObjectType","ObjectKey","Now() AS ExportTimestamp")
      )  
  except:
    
    msg_lines = str(sys.exc_info()[1]).split("\n")
    if len(msg_lines) <= 2:
      short_message = " ".join(msg_lines)
    else:
      short_message = " ".join(msg_lines[:2])   
    error_message = f"ERROR!!! : exception class {sys.exc_info()[0]},  message: {short_message}" #TODO collect more info, e.g. stack trace

    print(error_message)
    
    database_value = f"'ERROR_!!!_{database_name}'" if database_name else "NULL"
    object_key_value = f"'ERROR_!!!_{object_key}'" if object_key else "NULL"
    object_type_value = f"'ERROR_!!!_{object_type}'" if object_type else "NULL" # Import ignores this object type
    
    grants_df = spark.sql(f"""SELECT 
        {database_value} AS Database, 
        'ERROR_!!!' AS Principal,
        array("{error_message}") AS ActionTypes, 
        {object_type_value} AS ObjectType, 
        {object_key_value} AS ObjectKey, 
        Now() AS ExportTimestamp
     """)

  return grants_df
  

def create_table_ACLSs_df_for_databases(database_names: List[str]):
  
  # TODO check Catalog heuristic:
  #  if all databases are exported, we include the Catalog grants as well
  #. if only a few databases are exported: we exclude the Catalog
  if database_names is None or database_names == '':
    database_names = get_database_names()
    include_catalog = True
  else:
    include_catalog = False

  # ANONYMOUS FUNCTION
  combined_grant_dfs = create_grants_df(None, "ANONYMOUS FUNCTION", None)
  
  # ANY FILE
  combined_grant_dfs = combined_grant_dfs.unionAll(
    create_grants_df(None, "ANY FILE", None)
  )
  
  # CATALOG
  if include_catalog:
    combined_grant_dfs = combined_grant_dfs.unionAll(
      create_grants_df(None, "CATALOG", None)
    )
  #TODO ELSE: consider pushing catalog grants down to DB level in this case
  
  for database_name in database_names:
    
    # DATABASE
    combined_grant_dfs = combined_grant_dfs.unionAll(
      create_grants_df(database_name, "DATABASE", database_name)
    )
    
    tables_and_views_rows = spark.sql(
      f"SHOW TABLES IN {database_name}"
    ).filter(sf.col("isTemporary") == False).collect()
    
    print(f"{datetime.datetime.now()} working on database {database_name} with {len(tables_and_views_rows)} tables and views")
    for table_row in tables_and_views_rows:
      
      # TABLE, VIEW
      combined_grant_dfs = combined_grant_dfs.unionAll(
        create_grants_df(database_name, "TABLE", f"{table_row.database}.{table_row.tableName}")
      )
   
    #TODO ADD USER FUNCTION - not supported in SQL Analytics, so this can wait a bit
    # ... SHOW USER FUNCTIONS LIKE  <my db>.`*`;
    #. function_row['function']   ... nah does not seem to work
    
  #combined_grant_dfs = combined_grant_dfs.sort("")
      
  return combined_grant_dfs


# COMMAND ----------

# DBTITLE 1,Run Export
databases_raw = dbutils.widgets.get("Databases")
output_path = dbutils.widgets.get("OutputPath")

if databases_raw.rstrip() == '':
  databases = None
  print(f"Exporting all databases")
else:
  databases = [x.rstrip().lstrip() for x in databases_raw.split(",")]
  print(f"Exporting the following databases: {databases}")


table_ACLs_df = create_table_ACLSs_df_for_databases(databases)

print(f"{datetime.datetime.now()} writing table ACLs to {output_path}")

# with table ACLS active, I direct write to DBFS is not allowed, so we store
# the dateframe as a table for single zipped JSON file sorted, for consitent file diffs
(
  table_ACLs_df
 .coalesce(1)
 .selectExpr("Database","Principal","ActionTypes","ObjectType","ObjectKey","ExportTimestamp")
 .sort("Database","Principal","ObjectType","ObjectKey")
 .write
 .format("JSON")
 .option("compression","gzip")
 .mode("overwrite")
 .save(output_path)
)


# COMMAND ----------

# DBTITLE 1,Exported Table ACLs
display(spark.read.format("json").load(output_path)) 

# COMMAND ----------

print(output_path)

# COMMAND ----------

# DBTITLE 1,Write total_num_acls and num_errors to the notebook exit value
totals_df = (spark.read
      .format("JSON")
      .load(output_path)
      .selectExpr(
         "sum(1) AS total_num_acls"
        ,"sum(CASE WHEN Principal = 'ERROR_!!!' THEN 1 ELSE 0 END) AS num_errors")
     )

res_rows = totals_df.collect()

if len(res_rows) == 1:
  exit_JSON_string = '{ "total_num_acls": '+str(res_rows[0]["total_num_acls"])+', "num_errors": '+str(res_rows[0]["num_errors"])+' }'
else:
  exit_JSON_string = '{ "total_num_acls": -1, "num_errors": -1 }'

print(exit_JSON_string)

dbutils.notebook.exit(exit_JSON_string) 

# COMMAND ----------



# Databricks notebook source
# MAGIC %md #TestExportRawTableAcls
# MAGIC 
# MAGIC RawTableAcls Dump used for testing

# COMMAND ----------

##dbutils.widgets.removeAll()
dbutils.widgets.text("Databases","tomi_schumacher_adl_test, tomi_schumacher_adl_test_restricted","1: Databases (opt)")
dbutils.widgets.text("OutputPath","dbfs:/user/hive/warehouse/dbfs:/user/hive/warehouse/tomi_table_acl_raw_pre.delta","2: Output Path")

# COMMAND ----------

import datetime;
import pyspark.sql.functions as sf
from typing import Callable, Iterator, Union, Optional, List


def create_grants_df(database_name: str,object_type: str, object_key: str) -> List[str]:
  if object_type == "CATALOG": 
     grants_df = (
       spark.sql(f"SHOW GRANT ON CATALOG")
       .groupBy("ObjectType","ObjectKey","Principal")
         .agg(sf.collect_set("ActionType").alias("ActionTypes"))
       .selectExpr("NULL AS Database"
                   ,"Principal","ObjectType","ObjectKey","ActionTypes")
     )
  else: 
    grants_df = (
      spark.sql(f"SHOW GRANT ON {object_type} {object_key}")
      .filter(sf.col("ObjectType") == f"{object_type}")
      .groupBy("ObjectType","ObjectKey","Principal")
        .agg(sf.collect_set("ActionType").alias("ActionTypes"))
      .selectExpr(f"'{database_name}' AS Database"
                  ,"Principal","ObjectType","ObjectKey","ActionTypes")
    )  
  return grants_df


def collect_all_db_names():
    database_names = []
    for db in spark.sql("show databases").collect():
      if hasattr(db,"databaseName"): #Angela has this fallback ...
        database_names.append(db.databaseName)
      else:
        database_names.append(db.namespace)
    return database_names
  
  
def create_raw_table_ACLs_df(database_names: List[str]):
  combined_grant_dfs = create_grants_df(None, "CATALOG", None)

  if database_names is None or database_names == '': 
    database_names = collect_all_db_names()
    
  for database_name in database_names:
    print(f"Database: {database_name}")
    combined_grant_dfs = combined_grant_dfs.unionAll(
      create_grants_df(database_name, "DATABASE", database_name))
    
    tables_and_views_rows = spark.sql(
      f"SHOW TABLES IN {database_name}").filter(sf.col("isTemporary") == False).collect()

    for table_row in tables_and_views_rows:
      qualified_table_name = f"{table_row.database}.{table_row.tableName}"
      print(f"Table\View: {qualified_table_name}")
      combined_grant_dfs = combined_grant_dfs.unionAll(
        create_grants_df(database_name, "TABLE", qualified_table_name))
      
  return combined_grant_dfs

#database_names = ["tomi_schumacher_adl_test", "tomi_schumacher_adl_test_restricted"]
      
#combined_grant_dfs = create_raw_table_ACLs_df(database_names)

#display(combined_grant_dfs)

# COMMAND ----------

databases_raw = dbutils.widgets.get("Databases")
output_path = dbutils.widgets.get("OutputPath")

if databases_raw.strip() == '':
  databases = None
else:
  databases = [x.strip() for x in databases_raw.split(",")]
  
raw_table_ACLs_df = create_raw_table_ACLs_df(databases)

print(f"{datetime.datetime.now()} writing table ACLs to {output_path}")

raw_table_ACLs_df.write.format("DELTA").mode("overwrite").save(output_path)

# COMMAND ----------

display(spark.sql(f"SELECT * FROM delta.`{output_path}`"))
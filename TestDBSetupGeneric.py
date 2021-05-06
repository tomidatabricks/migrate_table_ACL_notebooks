# Databricks notebook source
# MAGIC %md #TestDBSetupGeneric
# MAGIC 
# MAGIC This notebook that creates a database for testing the `Export_Table_ACLs` and `Import_Table_ACLs` notebooks.
# MAGIC 
# MAGIC Paramaters:
# MAGIC - **SQLCommands**  `<SQL COMMAND>`( `;` `<SQL COMMAND>`)* to be executed. Typically: Create Database, Table, View,
# MAGIC   Grant, Deny, Alter Owner ...
# MAGIC - **SkipGrantDenyAlterOwner** `True` or `False` if set to true Grant/Deny/Alter Owner commands will be skipped
# MAGIC   this is used to generate the database without ACLs before importing the ACLs.
# MAGIC 
# MAGIC Pass in the same **SQLCommands** for both, set **Skip Grant/Deny/Alter Owner** to `True`for the import
# MAGIC and to `False` for the export case.
# MAGIC 
# MAGIC 
# MAGIC For this setup we assume the following users and groups exist:
# MAGIC 
# MAGIC users: 
# MAGIC - tomi.schumacher@databricks.com  (admin)
# MAGIC - tomi.schumacher+noadmin@databricks.com (non admin)
# MAGIC 
# MAGIC groups:
# MAGIC - users (default)
# MAGIC   - everybody
# MAGIC - tomi-acl-test-group
# MAGIC   - tomi.schumacher+noadmin@databricks.com
# MAGIC   
# MAGIC   
# MAGIC %md
# MAGIC ```
# MAGIC (base) tomi.schumacher@C02ZT3K6MD6M ~ % databricks groups list-parents --group-name tomi-acl-test-group
# MAGIC {}
# MAGIC 
# MAGIC (base) tomi.schumacher@C02ZT3K6MD6M ~ % databricks groups list-members  --group-name tomi-acl-test-group
# MAGIC {
# MAGIC   "members": [
# MAGIC     {
# MAGIC       "user_name": "tomi.schumacher+noadmin@databricks.com"
# MAGIC     }
# MAGIC   ]
# MAGIC }
# MAGIC 
# MAGIC ```

# COMMAND ----------

# MAGIC %md ##TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO 
# MAGIC 
# MAGIC - create admin and non admin user to test
# MAGIC   - tomi.schumacher+admin@databricks.com
# MAGIC   - make regular account non admin
# MAGIC   - create access token for tomi.schumacher+admin@databricks.com on https://e2-demo-west.cloud.databricks.com/

# COMMAND ----------

SQL_STATEMENTS_DEFAULT = """

-- Create Database 1 : tomi_schumacher_adl_test
DROP DATABASE IF EXISTS tomi_schumacher_adl_test CASCADE;
CREATE DATABASE tomi_schumacher_adl_test;

CREATE TABLE tomi_schumacher_adl_test.table_a (a INT) USING DELTA;


-- Create Database 2 : tomi_schumacher_adl_test_restricted
DROP DATABASE IF EXISTS tomi_schumacher_adl_test_restricted CASCADE;
CREATE DATABASE tomi_schumacher_adl_test_restricted;


CREATE TABLE tomi_schumacher_adl_test_restricted.regions USING DELTA AS
SELECT col1 AS region FROM VALUES ('North'),('South');
 
CREATE VIEW tomi_schumacher_adl_test_restricted.north_regions AS
SELECT region FROM tomi_schumacher_adl_test_restricted.regions WHERE region = 'North';

CREATE FUNCTION tomi_schumacher_adl_test_restricted.testUDF AS
'org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs';


-- Change Onwer 
ALTER DATABASE tomi_schumacher_adl_test_restricted
OWNER TO `mwc+user2@databricks.com`;

ALTER TABLE tomi_schumacher_adl_test_restricted.regions
OWNER TO `mwc+user2@databricks.com`;

ALTER VIEW tomi_schumacher_adl_test_restricted.north_regions
OWNER TO `mwc+user2@databricks.com`;


-- Change Privileges 
DENY ALL PRIVILEGES 
ON DATABASE tomi_schumacher_adl_test_restricted TO users;

-- ???? Do I need to Deny privileges on tables and views in database as well, or is this inherited ?


GRANT SELECT, MODIFY 
ON TABLE tomi_schumacher_adl_test_restricted.regions TO `mwc+user2@databricks.com`;

GRANT SELECT
ON VIEW tomi_schumacher_adl_test_restricted.north_regions TO `tomi-acl-test-group`;

-- TODO Figure out how to use a GRANT on FUNCTION 
--GRANT CREATE_NAMED_FUNCTION, MODIFY_CLASSPATH 


DENY ALL PRIVILEGES 
ON FUNCTION tomi_schumacher_adl_test_restricted.testUDF TO users;

GRANT SELECT
ON FUNCTION tomi_schumacher_adl_test_restricted.testUDF TO `tomi.schumacher@databricks.com`;

-- ANY FILE
GRANT MODIFY 
ON ANY FILE TO `tomi-acl-test-group`;

-- ANONYMOUS FUNCTION
GRANT SELECT 
ON ANONYMOUS FUNCTION TO `tomi-acl-test-group`;

"""

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("SQLCommands",SQL_STATEMENTS_DEFAULT)
dbutils.widgets.text("SkipGrantDenyAlterOwner","False")

# COMMAND ----------

import re

sql_commands = dbutils.widgets.get("SQLCommands")
skip_grant_deny_alter_owner = dbutils.widgets.get("SkipGrantDenyAlterOwner").lower() == "true"


# COMMAND ----------

def execute_sql_statements(sqls):
  # filter out SQL on comments
  sqls = re.sub('--[^\n]*','',sqls,flags=re.MULTILINE)
  for sql in sqls.split(sep=";"):
    sql = sql.strip()
    if sql:
      if skip_grant_deny_alter_owner and re.search("(?i)^(GRANT|DENY|ALTER)", sql): #TO add OWNER check part 
        print(f"/* SKIP>>\n{sql}\n<<SKIP */")
      else: # execute the statement
        print(f"{sql};")
        spark.sql(sql)


# COMMAND ----------

execute_sql_statements(sql_commands)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SHOW GRANT ON ANY FILE

# COMMAND ----------

# MAGIC %sql SHOW GRANT ON ANONYMOUS FUNCTION

# COMMAND ----------

# MAGIC %sql GRANT SELECT  ON ANONYMOUS FUNCTION TO `tomi-acl-test-group`;

# COMMAND ----------

# MAGIC %sql 
# MAGIC --tomi_schumacher_adl_test_restricted.testUDF
# MAGIC SHOW USER FUNCTIONS tomi_schumacher_adl_test_restricted.testUDF;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SHOW USER FUNCTIONS LIKE  tomi_schumacher_adl_test_restricted.`*`;

# COMMAND ----------

"""%sql 

USE tomi_schumacher_adl_test_restricted;
SHOW GRANT ON FUNCTION testudf;
USE default;
"""

# COMMAND ----------

# MAGIC %sql SHOW USER FUNCTIONS LIKE  tomi_schumacher_adl_test.`*`;

# COMMAND ----------

# MAGIC %sql SHOW GRANT `mwc+user2@databricks.com` ON VIEW tomi_schumacher_adl_test_restricted.north_regions;

# COMMAND ----------

# MAGIC %sql SHOW GRANT `tomi-acl-test-group` ON VIEW tomi_schumacher_adl_test_restricted.north_regions;

# COMMAND ----------

# MAGIC %sql SHOW GRANT ON ANY FILE

# COMMAND ----------

# MAGIC %sql SHOW GRANT ON VIEW tomi_schumacher_adl_test_restricted.north_regions

# COMMAND ----------

# MAGIC %sql SHOW GRANT ON DATABASE tomi_schumacher_adl_test_restricted

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM tomi_schumacher_adl_test_restricted.north_regions

# COMMAND ----------



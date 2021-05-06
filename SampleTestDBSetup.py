# Databricks notebook source
# MAGIC %md #SampleTestDBSetup
# MAGIC 
# MAGIC This notebook creates a couple of drops and creates a couple of
# MAGIC databases, tables and view.
# MAGIC It executes GRANT and DENY statements to configure Table ACLs
# MAGIC 
# MAGIC In parameter **Skip Grant/Deny/Alter Owner** configure whether GRANT and DENY statements will be 
# MAGIC executed or not: This is used for the testing harness
# MAGIC 
# MAGIC For this setup we assume the following users and groups exist:
# MAGIC 
# MAGIC users: 
# MAGIC - tomi.schumacher@databricks.com  (admin)
# MAGIC - mwc+user2@databricks.com (non admin)
# MAGIC 
# MAGIC groups:
# MAGIC - users (default)
# MAGIC   - everybody
# MAGIC - tomi-acl-test-group
# MAGIC   - mwc+user2@databricks.com
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
# MAGIC       "user_name": "mwc+user2@databricks.com"
# MAGIC     }
# MAGIC   ]
# MAGIC }
# MAGIC 
# MAGIC ```

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("SkipGrantDenyAlterOwner","False","1: Skip GRANT/DENY/ALTER OWNER:")

# COMMAND ----------

import re

skip_grant_deny_alter_owner = dbutils.widgets.get("SkipGrantDenyAlterOwner").lower() == "true"


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

execute_sql_statements("""

-- DB1
DROP DATABASE IF EXISTS db_acl_test CASCADE;
CREATE DATABASE db_acl_test;

CREATE TABLE db_acl_test.table_a (a INT) USING DELTA;


-- DB2
DROP DATABASE IF EXISTS db_acl_test_restricted CASCADE;
CREATE DATABASE db_acl_test_restricted;


CREATE TABLE db_acl_test_restricted.regions USING DELTA AS
SELECT col1 AS region FROM VALUES ('North'),('South');
 
CREATE VIEW db_acl_test_restricted.north_regions AS
SELECT region FROM db_acl_test_restricted.regions WHERE region = 'North';

CREATE FUNCTION db_acl_test_restricted.testUDF AS
'org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs';


-- TODO ownership export/import seems buggy
ALTER DATABASE db_acl_test_restricted
OWNER TO `mwc+user2@databricks.com`;

DENY ALL PRIVILEGES 
ON DATABASE db_acl_test_restricted TO users;

GRANT SELECT, MODIFY 
ON TABLE db_acl_test_restricted.regions TO `mwc+user2@databricks.com`;

GRANT SELECT
ON VIEW db_acl_test_restricted.north_regions TO `tomi-acl-test-group`;

-- TODO Figure out how to use a GRANT on FUNCTION 
--GRANT CREATE_NAMED_FUNCTION, MODIFY_CLASSPATH 


DENY ALL PRIVILEGES 
ON FUNCTION db_acl_test_restricted.testUDF TO users;

GRANT SELECT
ON FUNCTION db_acl_test_restricted.testUDF TO `tomi.schumacher@databricks.com`;

-- ANY FILE
GRANT MODIFY 
ON ANY FILE TO `tomi-acl-test-group`;

-- ANONYMOUS FUNCTION
GRANT SELECT 
ON ANONYMOUS FUNCTION TO `tomi-acl-test-group`;

""")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SHOW GRANT ON ANY FILE

# COMMAND ----------

# MAGIC %sql SHOW GRANT ON ANONYMOUS FUNCTION

# COMMAND ----------

# MAGIC %sql GRANT SELECT  ON ANONYMOUS FUNCTION TO `tomi-acl-test-group`;

# COMMAND ----------

"""%sql 

USE db_acl_test_restricted;
SHOW GRANT ON FUNCTION testudf;
USE default;
"""

# COMMAND ----------

# MAGIC %sql SHOW GRANT `tomi.schumacher+noadmin@databricks.com` ON VIEW tomi_schumacher_adl_test_restricted.north_regions;

# COMMAND ----------

# MAGIC %sql SHOW GRANT ON VIEW db_acl_test_restricted.north_regions

# COMMAND ----------

# MAGIC %sql SHOW GRANT ON DATABASE db_acl_test_restricted

# COMMAND ----------



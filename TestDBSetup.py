# Databricks notebook source
# MAGIC %md #TestDBSetup
# MAGIC 
# MAGIC This notebook creates a couple of drops and creates a couple of
# MAGIC databases, tables and view.
# MAGIC It executes GRANT and DENY statements to configure Table ACLs
# MAGIC 
# MAGIC In parameter **Skip Grant/Deny** configure whether GRANT and DENY statements will be 
# MAGIC executed or not: This is used for the testing harness
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

dbutils.widgets.removeAll()
dbutils.widgets.text("SkipGrantDeny","False","1: Skip GRANT/DENY:")

# COMMAND ----------

import re

skip_grant_deny = dbutils.widgets.get("SkipGrantDeny").lower() == "true"


def execute_sql_statements(sqls):
  # filter out SQL on comments
  sqls = re.sub('--[^\n]*','',sqls,flags=re.MULTILINE)
  for sql in sqls.split(sep=";"):
    sql = sql.strip()
    if sql:
      if skip_grant_deny and re.search("(?i)^(GRANT|DENY)", sql):
        print(f"/* SKIP>>\n{sql}\n<<SKIP */")
      else: # execute the statement
        print(f"{sql};")
        spark.sql(sql)


# COMMAND ----------

execute_sql_statements("""

-- DB1
DROP DATABASE IF EXISTS tomi_schumacher_adl_test CASCADE;
CREATE DATABASE tomi_schumacher_adl_test;

CREATE TABLE tomi_schumacher_adl_test.table_a (a INT) USING DELTA;


-- DB2
DROP DATABASE IF EXISTS tomi_schumacher_adl_test_restricted CASCADE;
CREATE DATABASE tomi_schumacher_adl_test_restricted;


CREATE TABLE tomi_schumacher_adl_test_restricted.regions USING DELTA AS
SELECT col1 AS region FROM VALUES ('North'),('South');
 
CREATE VIEW tomi_schumacher_adl_test_restricted.north_regions AS
SELECT region FROM tomi_schumacher_adl_test_restricted.regions WHERE region = 'North';

CREATE FUNCTION tomi_schumacher_adl_test_restricted.testUDF AS
'org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs';


-- TODO ownership export/import seems buggy
ALTER DATABASE tomi_schumacher_adl_test_restricted
OWNER TO `tomi.schumacher+noadmin@databricks.com`;

DENY ALL PRIVILEGES 
ON DATABASE tomi_schumacher_adl_test_restricted TO users;

GRANT SELECT, MODIFY 
ON TABLE tomi_schumacher_adl_test_restricted.regions TO `tomi.schumacher+noadmin@databricks.com`;

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

# MAGIC %sql 
# MAGIC --tomi_schumacher_adl_test_restricted.testUDF
# MAGIC SHOW USER FUNCTIONS tomi_schumacher_adl_test_restricted.testUDF;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SHOW USER FUNCTIONS LIKE  tomi_schumacher_adl_test_restricted.`*`;

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC USE tomi_schumacher_adl_test_restricted;
# MAGIC SHOW GRANT ON FUNCTION testudf;
# MAGIC USE default;

# COMMAND ----------

# MAGIC %sql SHOW USER FUNCTIONS LIKE  tomi_schumacher_adl_test.`*`;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC USE default;
# MAGIC DROP FUNCTION tomi_foo;
# MAGIC CREATE FUNCTION tomi_foo AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs';
# MAGIC 
# MAGIC -- returns nothing
# MAGIC --SHOW USER FUNCTIONS LIKE 'default.tomi_foo'; 
# MAGIC 
# MAGIC --returns one
# MAGIC --SHOW USER FUNCTIONS LIKE 'tomi_foo'; 
# MAGIC --SHOW USER FUNCTIONS LIKE tomi_foo; 
# MAGIC --SHOW USER FUNCTIONS LIKE default.tomi_foo; 
# MAGIC --SHOW USER FUNCTIONS LIKE default.`tomi_foo`;
# MAGIC 
# MAGIC -- returns 4
# MAGIC --SHOW USER FUNCTIONS LIKE default.`*`;
# MAGIC 
# MAGIC -- returns + getargument
# MAGIC --SHOW USER FUNCTIONS LIKE tomi_schumacher_adl_test_restricted.`*`;
# MAGIC 
# MAGIC -- only getargument
# MAGIC SHOW USER FUNCTIONS LIKE tomi_schumacher_adl_test.`*`;

# COMMAND ----------

# MAGIC %sql SHOW USER FUNCTIONS tomi_foo

# COMMAND ----------

# MAGIC %sql SHOW USER FUNCTIONS LIKE tomi_foo;

# COMMAND ----------

# MAGIC %sql SHOW USER FUNCTIONS LIKE 'tomi_*';

# COMMAND ----------

# MAGIC %sql 

# COMMAND ----------

# MAGIC %sql
# MAGIC USE tomi_schumacher_adl_test;
# MAGIC SHOW USER FUNCTIONS;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE tomi_schumacher_adl_test_restricted;
# MAGIC SHOW USER FUNCTIONS LIKE '*';

# COMMAND ----------

# MAGIC %sql DESCRIBE FUNCTION EXTENDED  tomi_schumacher_adl_test_restricted.getargument;

# COMMAND ----------

# MAGIC %sql DESCRIBE FUNCTION EXTENDED  getargument;

# COMMAND ----------

# MAGIC %sql SHOW GRANT ON VIEW tomi_schumacher_adl_test_restricted.north_regions;
# MAGIC -- Principal ActionType ObjectType ObjectKey

# COMMAND ----------

# MAGIC %sql SHOW GRANT `tomi.schumacher+noadmin@databricks.com` ON VIEW tomi_schumacher_adl_test_restricted.north_regions;

# COMMAND ----------

# MAGIC %sql SHOW GRANT `tomi-acl-test-group` ON VIEW tomi_schumacher_adl_test_restricted.north_regions;

# COMMAND ----------

# MAGIC %sql SHOW GRANT ON ANY FILE

# COMMAND ----------

# MAGIC %sql SHOW GRANT `tomi-acl-test-group` ON ANY FILE

# COMMAND ----------

# MAGIC %sql SELECT NOW()

# COMMAND ----------


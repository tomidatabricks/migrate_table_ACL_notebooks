# Databricks notebook source
# MAGIC %md #Test Runner
# MAGIC 
# MAGIC Must pip install Databricks CLI on cluster (`pip install databricks-cli`)

# COMMAND ----------

import os
import json

# COMMAND ----------

# DBTITLE 1,Set Host and Token env vars for databricks cli
url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None) 
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
clusterId = dbutils.notebook.entry_point.getDbutils().notebook().getContext().clusterId().getOrElse(None)

os.environ["DATABRICKS_HOST"] = url
os.environ["DATABRICKS_TOKEN"] = token



# COMMAND ----------

with os.popen('databricks  jobs list') as stream:
  print(stream.read())
  #results = json.load(stream)

# COMMAND ----------

def run_notebook_on_cluster(cid, notebook_path, notebook_params):
    runs_submit_params = {
        "run_name": f"migrate runs submit (notebook_path)",
        "existing_cluster_id": cid,
        "notebook_task": {
            "notebook_path": notebook_path,
            "base_parameters": notebook_params
        }
    }
    
    json_str = json.dumps(runs_submit_params).replace('"',"'")
    command_line = 'databricks runs submit --json "' + json.dumps(runs_submit_params).replace('"',"'") + '"'
    print(f"command_line: \n{command_line}")
    
    
    with os.popen(command_line) as stream:
      res_str = stream.read()
      print(f"result: \n{res_str}")
      res = json.loads(res_str)    
      if res["http_status_code"] != 200:
        # TODO add more error_text
        raise Exception("Could not run submit notebook")

    return res["run_id"]

SRC_CLUSTER_ID = "0503-180150-pinup574"

run_notebook_on_cluster(SRC_CLUSTER_ID, "/Repos/tomi.schumacher@databricks.com/migrate_table_ACL_notebooks/Export_Table_ACLs.py",{
  "Databases": "tomi_schumacher_adl_test, tomi_schumacher_adl_test_restricted",
  "OutputPath": "dbfs:/tmp/migrate/tomi_table_acl_perms.json.gz"
})

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC for i in databricks databricks-results tmp FileStore databricks-datasets; do du -sh /dbfs/${i}; done

# COMMAND ----------



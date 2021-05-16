# Databricks notebook source
# MAGIC %md #Test Runner
# MAGIC 
# MAGIC Must pip install Databricks CLI on cluster (`pip install databricks-cli`)
# MAGIC 
# MAGIC 
# MAGIC This Notebook expects a single paramter JSON paramter named `ParamtersJSON` of the following form:
# MAGIC 
# MAGIC ```
# MAGIC {
# MAGIC   "source_databricks_host": "CURRENT",
# MAGIC   "source_databricks_token": "CURRENT",
# MAGIC   "source_ACL_cluster_name": "API_DBR_8_Table_ACL_Work_Leave_Me_Alone",
# MAGIC   "destination_databricks_host": "https://e2-demo-migrate-dst.cloud.databricks.com",
# MAGIC   "destination_databricks_token": "{{secrets/table_acl_tests/target_databricks_token}}",
# MAGIC   "destination_ACL_cluster_name": "API_DBR_8_Table_ACL_Work_Leave_Me_Alone"
# MAGIC }
# MAGIC ```
# MAGIC 
# MAGIC There are 3 forms for the values:
# MAGIC - `CURRENT` for host and token: the host and token used in this current notebook
# MAGIC - `{{secrets/<secret_scope>/<secret_key>}}` look up a a secrect by its scope and value
# MAGIC - `value` and other form, is taken as a literal value

# COMMAND ----------

import os
import json
import subprocess
import time
import re

RUN_TESTS = True # whether to run some test code or not to run some test

# COMMAND ----------

dbutils.widgets.removeAll()


# COMMAND ----------

# DBTITLE 1,Untitled
params_sample_map = {
  "source_databricks_host": "https://e2-demo-migrate-src.cloud.databricks.com",
  "source_databricks_token": "{{secrets/table_acl_tests/source_databricks_token}}",
  "source_ACL_cluster_name": "API_DBR_8_Table_ACL_Work_Leave_Me_Alone",
  "destination_databricks_host": "https://e2-demo-migrate-dst.cloud.databricks.com",
  "destination_databricks_token": "{{secrets/table_acl_tests/target__databricks_token}}",
  "destination_ACL_cluster_name": "API_DBR_8_Table_ACL_Work_Leave_Me_Alone"
}

params_sample_string = json.dumps(params_sample_map, indent=2)

dbutils.widgets.text("ParamtersJSON",params_sample_string,"1: ParamtersJSON")



print(json.dumps(json.loads(dbutils.widgets.get("ParamtersJSON")),indent=2))

# COMMAND ----------

params_sample['destination_databricks_token']

# COMMAND ----------

# DBTITLE 1,Set Host and Token env vars for databricks cli
URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None) 
BROWSER_HOST_NAME = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().getOrElse(None)
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
#CLUSTER_ID = dbutils.notebook.entry_point.getDbutils().notebook().getContext().clusterId().getOrElse(None)


PARAMETERS = json.loads(dbutils.widgets.get("ParamtersJSON"))

def process_token_value(value, current_value):
  SECRETS_REGEXP = "^\{\{\W*secrets\W*/\W*(\w*)\W*/\W*(\w*)\W*\}\}$"
  res = value
  if value == 'CURRENT':
    res = current_value
  else:
    match =  re.search(SECRETS_REGEXP, value)
    if match:
      res = dbutils.secrets.get(match.group(1),match.group(2))
  return res

PARAMETERS['source_databricks_host'] = process_token_value(PARAMETERS['source_databricks_host'],URL) 
PARAMETERS['source_databricks_token'] = process_token_value(PARAMETERS['source_databricks_token'],TOKEN) 
PARAMETERS['source_ACL_cluster_name'] = process_token_value(PARAMETERS['source_ACL_cluster_name'],'need_source_ACL_cluster_name') 


PARAMETERS['destination_databricks_host'] = process_token_value(PARAMETERS['destination_databricks_host'],URL) 
PARAMETERS['destination_databricks_token'] = process_token_value(PARAMETERS['destination_databricks_token'],TOKEN) 
PARAMETERS['destination_ACL_cluster_name'] = process_token_value(PARAMETERS['destination_ACL_cluster_name'],'need_destination_ACL_cluster_name') 



#os.environ["DATABRICKS_HOST"] = URL
#os.environ["DATABRICKS_TOKEN"] = TOKEN

# COMMAND ----------

print(json.dumps(dbutils.widgets.get("ParamtersJSON"),indent=2))

# COMMAND ----------

# DBTITLE 1,Create ~/.databrickscfg file to use the databricks cli in the webterminal
# TODO: there must be a more pythonic way to create or replace a multi line file
file_content = f"""
[DEFAULT]
host = {PARAMETERS['source_databricks_host']}
token = {PARAMETERS['source_databricks_token']}

[src]
host = {PARAMETERS['source_databricks_host']}
token = {PARAMETERS['source_databricks_token']}

[dst]
host = {PARAMETERS['destination_databricks_host']}
token = {PARAMETERS['destination_databricks_token']}
"""

with open("/root/.databrickscfg", "w") as f:
  f.seek(0)
  f.write(file_content)
  f.truncate()

# COMMAND ----------

def run_it(command_list, verbose=False, parse_output=True):
  if verbose: 
    print(' '.join(command_list))
  call = subprocess.run(command_list, stdout=subprocess.PIPE, text=True, check=True)
  if parse_output:
    try:
      res = json.loads(call.stdout)
    except: #it was not a JSON response
      lines = call.stdout.strip().split('\n')
      #res = lines
      res = [line.split()  for line in lines]
  else:
    res = call.stdout
  if verbose: 
    print(call.stdout)
  return res
  
if RUN_TESTS:

  run_it(["databricks", "workspace","ls","--long","--absolute","/Users/"],verbose=True)

  jobs_res = run_it("databricks jobs list".split(),verbose=True)

  job = run_it(["databricks","jobs","get","--job-id",jobs_res[0][0]],verbose=True)
  print(json.dumps(job, indent=2))

  #run_it(['databricks','runs','--help'],verbose=True,parse_output=False)

# COMMAND ----------

SOURCE_CLUSTER = run_it(f"databricks --profile src clusters get --cluster-name {PARAMETERS['source_ACL_cluster_name']}".split(), verbose=True)
DESTINATION_CLUSTER = run_it(f"databricks --profile dst clusters get --cluster-name {PARAMETERS['destination_ACL_cluster_name']}".split(), verbose=True)



# COMMAND ----------



def run_notebook_on_cluster(cid, notebook_path, notebook_params, sync=True, verbose=False):
    runs_submit_params = {
        "run_name": f"Test Runner Runs: {notebook_path}",
        "existing_cluster_id": cid,
        "notebook_task": {
            "notebook_path": notebook_path,
            "base_parameters": notebook_params
        }
    }
    res = run_it(['databricks','runs','submit','--json',json.dumps(runs_submit_params)],verbose=verbose)
    
    if sync: # Poll job until it completed  
      while True:
        res = run_it(['databricks','runs','get','--run-id',str(res["run_id"])],verbose=verbose)
        # https://e2-demo-west.cloud.databricks.com/?o=2556758628403379#job/1515/run/1
        print(f"https://e2-demo-migrate-src.cloud.databricks.com/?#job/{res['job_id']}/run/{res['number_in_job']}")
        if res['state']['life_cycle_state'] not in ['PENDING','RUNNING','TERMINATING']:
          break
        time.sleep(2)
    return res
  
if RUN_TESTS:  

  SRC_CLUSTER_ID = "0503-180150-pinup574"

  notebook_params = {
    "Databases": "tomi_schumacher_adl_test, tomi_schumacher_adl_test_restricted",
    "OutputPath": "dbfs:/tmp/migrate/tomi_table_acl_perms.json.gz"
  }

  run = run_notebook_on_cluster(SRC_CLUSTER_ID, "/Repos/tomi.schumacher@databricks.com/migrate_table_ACL_notebooks/Export_Table_ACLs",notebook_params,verbose=True)
  
  

  if run['state']['result_state'] == 'SUCCESS':
    print("\n\nSUCCESS")
    #run_it(['databricks','fs','ls','--absolute','-l',notebook_params['OutputPath']],verbose=True)
    run_it(f"databricks fs ls --absolute -l {notebook_params['OutputPath']}".split(),verbose=True)
  else:
    print("\n\nERROR")

# COMMAND ----------

run_it(f"databricks fs ls --absolute -l {notebook_params['OutputPath']}/*.json.gz".split(),verbose=False)

# COMMAND ----------


res = run_it("databricks clusters get --cluster-name API_DBR_8_Table_ACL_Work_Leave_Me_Alone".split())
print(res["cluster_id"])

# COMMAND ----------

run_it([
  'databricks','workspace','list','--long','--absolute'
  ,'/Repos/tomi.schumacher@databricks.com/migrate_table_ACL_notebooks/Import_Table_ACLs'],verbose=True,parse_output=False)

# COMMAND ----------



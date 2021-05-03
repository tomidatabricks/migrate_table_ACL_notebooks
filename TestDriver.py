# Databricks notebook source
import json, os, datetime, requests
import requests.packages.urllib3

global pprint_j

requests.packages.urllib3.disable_warnings()


# Helper to pretty print json
def pprint_j(i):
    print(json.dumps(i, indent=4, sort_keys=True))


class dbclient:
    """
    Rest API Wrapper for Databricks APIs
    """
    # set of http error codes to throw an exception if hit. Handles client and auth errors
    http_error_codes = (401, 403)

    def __init__(self, token, url, is_verbose=False):
        self._token = {'Authorization': 'Bearer {0}'.format(token)}
        self._url = url.rstrip("/")
        self._is_verbose = is_verbose
        self._verify_ssl = False
        if self._verify_ssl:
            # set these env variables if skip SSL verification is enabled
            os.environ['REQUESTS_CA_BUNDLE'] = ""
            os.environ['CURL_CA_BUNDLE'] = ""

    def is_aws(self):
        return self._is_aws

    def is_verbose(self):
        return self._is_verbose

    def is_skip_failed(self):
        return self._skip_failed

    def test_connection(self):
        # verify the proper url settings to configure this client
        if self._url[-4:] != '.com' and self._url[-4:] != '.net':
            print("Hostname should end in '.com'")
            return -1
        results = requests.get(self._url + '/api/2.0/clusters/spark-versions', headers=self._token,
                               verify=self._verify_ssl)
        http_status_code = results.status_code
        if http_status_code != 200:
            print("Error. Either the credentials have expired or the credentials don't have proper permissions.")
            print("If you have a ~/.netrc file, check those credentials. Those take precedence over passed input.")
            print(results.text)
            return -1
        return 0

    def get(self, endpoint, json_params=None, version='2.0', print_json=False):
        if version:
            ver = version
        full_endpoint = self._url + '/api/{0}'.format(ver) + endpoint
        if self.is_verbose():
            print("Get: {0}".format(full_endpoint))
        if json_params:
            raw_results = requests.get(full_endpoint, headers=self._token, params=json_params, verify=self._verify_ssl)
            http_status_code = raw_results.status_code
            if http_status_code in dbclient.http_error_codes:
                raise Exception("Error: GET request failed with code {}\n{}".format(http_status_code, raw_results.text))
            results = raw_results.json()
        else:
            raw_results = requests.get(full_endpoint, headers=self._token, verify=self._verify_ssl)
            http_status_code = raw_results.status_code
            if http_status_code in dbclient.http_error_codes:
                raise Exception("Error: GET request failed with code {}\n{}".format(http_status_code, raw_results.text))
            results = raw_results.json()
        if print_json:
            print(json.dumps(results, indent=4, sort_keys=True))
        if type(results) == list:
            results = {'elements': results}
        results['http_status_code'] = raw_results.status_code
        return results

    def http_req(self, http_type, endpoint, json_params, version='2.0', print_json=False, files_json=None):
        if version:
            ver = version
        full_endpoint = self._url + '/api/{0}'.format(ver) + endpoint
        if self.is_verbose():
            print("{0}: {1}".format(http_type, full_endpoint))
        if json_params:
            if http_type == 'post':
                if files_json:
                    raw_results = requests.post(full_endpoint, headers=self._token,
                                                data=json_params, files=files_json, verify=self._verify_ssl)
                else:
                    raw_results = requests.post(full_endpoint, headers=self._token,
                                                json=json_params, verify=self._verify_ssl)
            if http_type == 'put':
                raw_results = requests.put(full_endpoint, headers=self._token,
                                           json=json_params, verify=self._verify_ssl)
            if http_type == 'patch':
                raw_results = requests.patch(full_endpoint, headers=self._token,
                                             json=json_params, verify=self._verify_ssl)
            
            http_status_code = raw_results.status_code
            if http_status_code in dbclient.http_error_codes:
                raise Exception("Error: {0} request failed with code {1}\n{2}".format(http_type,
                                                                                      http_status_code,
                                                                                      raw_results.text))
            results = raw_results.json()
        else:
            print("Must have a payload in json_args param.")
            return {}
        if print_json:
            print(json.dumps(results, indent=4, sort_keys=True))
        # if results are empty, let's return the return status
        if results:
            results['http_status_code'] = raw_results.status_code
            return results
        else:
            return {'http_status_code': raw_results.status_code}

    def post(self, endpoint, json_params, version='2.0', print_json=False, files_json=None):
        return self.http_req('post', endpoint, json_params, version, print_json, files_json)

    def put(self, endpoint, json_params, version='2.0', print_json=False):
        return self.http_req('put', endpoint, json_params, version, print_json)

    def patch(self, endpoint, json_params, version='2.0', print_json=False):
        return self.http_req('patch', endpoint, json_params, version, print_json)

    @staticmethod
    def my_map(F, items):
        to_return = []
        for elem in items:
            to_return.append(F(elem))
        return to_return

    def set_export_dir(self, dir_location):
        self._export_dir = dir_location

    def get_export_dir(self):
        return self._export_dir

    def get_latest_spark_version(self):
        versions = self.get('/clusters/spark-versions')['versions']
        v_sorted = sorted(versions, key=lambda i: i['key'], reverse=True)
        for x in v_sorted:
            img_type = x['key'].split('-')[1][0:5]
            if img_type == 'scala':
                return x


# COMMAND ----------

import time

POLLING_INTERVAL = 3

class table_ACLs_test_driver(dbclient):
    
    def get_cluster_id_by_name(self, cluster_name):
      cluster_id = None
      
      clusters = self.get('/clusters/list').get('clusters',[])
      filtered_clusters = list(filter(lambda x: x.get('cluster_name', None) == cluster_name, clusters))
      if len(filtered_clusters) == 1:
        cluster_id = filtered_clusters[0]['cluster_id']
        
      return cluster_id
    
    
    def execute_notebook_on_cluster_sync(self, clusterId, notebook_path, notebook_params):
      runs_submit_params = {
          "run_name": "TestDriver runs_submit",
          "existing_cluster_id": clusterId,
          "notebook_task": {
            "notebook_path": notebook_path,
            "base_parameters": notebook_params
          }
        }
      res = self.post('/jobs/runs/submit', runs_submit_params, print_json=False)
      
      if res["http_status_code"] == 200:
        run_id = res["run_id"]

        while True: #pull until job terminated
          res = self.get('/jobs/runs/get', {'run_id':run_id}, print_json=False)
          if self.is_verbose():
             print(f"polling for job to finish: {res['run_page_url']}")
          if res["http_status_code"] != 200 or res["state"]['life_cycle_state'] == 'TERMINATED':
            break
          time.sleep(POLLING_INTERVAL)
          
      return res
    
    
    def get_num_users(self):
      users = self.get('/preview/scim/v2/Users').get('Resources', [])
      return len(users)
    
    
    def get_num_groups(self):
      groups = self.get('/preview/scim/v2/Groups').get('Resources', [])
      return len(groups)
    

# COMMAND ----------

url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None) 
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
clusterId = dbutils.notebook.entry_point.getDbutils().notebook().getContext().clusterId().getOrElse(None)
client = table_ACLs_test_driver(token, url, is_verbose=False)

TABLE_ACLS_CLUSTER_NAME='TomiACLExport'
table_acl_clusterId = client.get_cluster_id_by_name(TABLE_ACLS_CLUSTER_NAME)

NOTEBOOK_DIR="/Users/tomi.schumacher@databricks.com/ACL/"

def exec_notebook(notebook_name, notebook_params):
  print(f"\nExecuting {notebook_name}({notebook_params})...")
  res = client.execute_notebook_on_cluster_sync(
    table_acl_clusterId, NOTEBOOK_DIR+notebook_name, notebook_params
  )  
  return res



# COMMAND ----------




exec_notebook("TestDBSetup", { 
  'SkipGrantDeny' : 'False'
})

exec_notebook("TestExportRawTableACLs", { 
  'Databases' : 'tomi_schumacher_adl_test, tomi_schumacher_adl_test_restricted'
  ,'OutputPath' : 'dbfs:/user/hive/warehouse/tomi_table_acls_raw_before.delta'
})

exec_notebook("Export_Table_ACLs", { 
  'Databases' : 'tomi_schumacher_adl_test, tomi_schumacher_adl_test_restricted'
  ,'OutputPath' : 'dbfs:/user/hive/warehouse/tomi_table_acl_perms.json'
  ,'OuputFormat' : 'JSON'
})

# recreate the original database without table ACLs, so they can be re-imported
exec_notebook("TestDBSetup", { 
  'SkipGrantDeny' : 'True'
})

exec_notebook("TestExportRawTableACLs", { 
  'Databases' : 'tomi_schumacher_adl_test, tomi_schumacher_adl_test_restricted'
  ,'OutputPath' : 'dbfs:/user/hive/warehouse/tomi_table_acls_raw_wihout_acls.delta'
})


exec_notebook("Import_Table_ACLs", { 
  'ImportPath' : 'dbfs:/user/hive/warehouse/tomi_table_acl_perms.json'
  ,'ImportPath' : 'JSON'
})

exec_notebook("TestExportRawTableACLs", { 
  'Databases' : 'tomi_schumacher_adl_test, tomi_schumacher_adl_test_restricted'
  ,'OutputPath' : 'dbfs:/user/hive/warehouse/tomi_table_acls_raw_after_import.delta'
})


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "#raw ACLS before export", count(1) FROM delta.`dbfs:/user/hive/warehouse/tomi_table_acls_raw_before.delta`
# MAGIC UNION ALL
# MAGIC SELECT "#raw ACLS before import", count(1) FROM delta.`dbfs:/user/hive/warehouse/tomi_table_acls_raw_wihout_acls.delta`
# MAGIC UNION ALL
# MAGIC SELECT "#raw ACLS after import", count(1) FROM delta.`dbfs:/user/hive/warehouse/tomi_table_acls_raw_after_import.delta`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE delta.`dbfs:/user/hive/warehouse/tomi_table_acls_raw_before.delta`

# COMMAND ----------

import pyspark.sql.functions as sf

def diff(name1, df1 , name2, df2 ):
  df1_without_df2 = df1.subtract(df2).withColumn("unique", sf.lit(name1))
  df2_without_df1 = df2.subtract(df1).withColumn("unique", sf.lit(name2))
  return df1_without_df2.unionAll(df2_without_df1)
  
  
df1 = spark.read.format('delta').load("dbfs:/user/hive/warehouse/tomi_table_acls_raw_before.delta")  
#df2 = spark.read.format('delta').load("dbfs:/user/hive/warehouse/tomi_table_acls_raw_wihout_acls.delta") 
df2 = spark.read.format('delta').load("dbfs:/user/hive/warehouse/tomi_table_acls_raw_after_import.delta") 
diff = diff("exported",df1,"imported",df2)
display(diff)

# COMMAND ----------

collected_diffs = diff.collect()

print('='*80)
if collected_diffs:
  print(f"ERRORS : total number of differences: {size(collected_diffs)}")
else:
  print(f"SUCCESS : no differences found")
print('='*80)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`dbfs:/user/hive/warehouse/tomi_table_acls_raw_before.delta` 
# MAGIC WHERE 
# MAGIC    Database = 'tomi_schumacher_adl_test_restricted'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`dbfs:/user/hive/warehouse/tomi_table_acls_raw_after_import.delta` 
# MAGIC WHERE 
# MAGIC    Database = 'tomi_schumacher_adl_test_restricted'

# COMMAND ----------


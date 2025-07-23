# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "08715978-21cf-4596-b843-302dee41e8fd",
          "fs.azure.account.oauth2.client.secret": "_~4UWpojLW9~J_v7WU3v-.g-TmcOYs677O",
          "fs.azure.account.oauth2.client.endpoint": "https://login.chinacloudapi.cn/b388b808-0ec9-4a09-a414-a7cbbd8b7e9b/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://adftest@storage0331.dfs.core.chinacloudapi.cn/test02/",
  mount_point = "/mnt/mount01",
  extra_configs = configs)

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.storage0331.dfs.core.chinacloudapi.cn", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.storage0331.dfs.core.chinacloudapi.cn", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.storage0331.dfs.core.chinacloudapi.cn", "st=2025-05-28T02%3A54%3A38Z&se=2025-05-28T03%3A54%3A38Z&sp=rl&sv=2025-05-05&sr=d&sdd=1&sig=1Pva5rCodrSRF%2BznXRZRryblLlxygOTDG27S0wLq4BA%3D")

# COMMAND ----------

dbutils.fs.ls("abfss://container01@storage0331.dfs.core.chinacloudapi.cn/test/")

# COMMAND ----------

configs = {"fs.azure.account.auth.type.storage0331.dfs.core.chinacloudapi.cn": "OAuth",
          "fs.azure.sas.token.provider.type.storage0331.dfs.core.chinacloudapi.cn": "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider",
          "fs.azure.sas.fixed.token.storage0331.dfs.core.chinacloudapi.cn": "st=2025-05-28T02%3A54%3A38Z&se=2025-05-28T03%3A54%3A38Z&sp=rl&sv=2025-05-05&sr=d&sdd=1&sig=1Pva5rCodrSRF%2BznXRZRryblLlxygOTDG27S0wLq4BA%3D"}
 
# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://container01@storage0331.dfs.core.chinacloudapi.cn/test/",
  mount_point = "/mnt/mount03",
  extra_configs = configs)

# COMMAND ----------



# COMMAND ----------

# 配置参数
storage_account_name = "storage0331"
container_name = "container01"
sas_token = "st=2025-05-28T02%3A54%3A38Z&se=2025-05-28T03%3A54%3A38Z&sp=rl&sv=2025-05-05&sr=d&sdd=1&sig=1Pva5rCodrSRF%2BznXRZRryblLlxygOTDG27S0wLq4BA%3D"  # 例如 "?sv=2022-11-02&ss=...&sig=..."

# 挂载路径
mount_point = "/mnt/<自定义挂载点名称>"

# 挂载命令
dbutils.fs.mount(
  source = f"wasbs://{container_name}@{storage_account_name}.dfs.core.chinacloudapi.cn",
  mount_point = mount_point,
  extra_configs = {f"fs.azure.sas.{container_name}.{storage_account_name}.dfs.core.chinacloudapi.cn": sas_token}
)

# 验证挂载
display(dbutils.fs.ls(mount_point))

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

df = spark.read.format("pdf") \
.option("imageType", "BINARY") \
.option("resolution", "200") \
.option("pagePerPartition", "2") \
.option("reader", "pdfBox") \
.load("path to the pdf file(s)")

df.select("path", "document").show()

# COMMAND ----------

dbutils.fs.unmount("/mnt/mount01")

# COMMAND ----------

dbutils.fs.ls("/mnt/mount01")

# COMMAND ----------

dbutils.fs.refreshMounts()

# COMMAND ----------

df = spark.read.format("pdf") \
.option("imageType", "BINARY") \
.option("resolution", "200") \
.option("pagePerPartition", "2") \
.option("reader", "pdfBox") \
.load("dbfs:/mnt/mount01/64616f923ac929a13be9f1f98662c118.pdf")

df.select("path", "document").show()

# COMMAND ----------

spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.secrets.get(scope="scope0427", key="secret0427"))

# COMMAND ----------

dbutils.fs.unmount("/mnt/mount01")

# COMMAND ----------

dbutils.secrets.get(scope="scope0427", key="secret0427")

# COMMAND ----------

password = dbutils.secrets.get(scope = "scope0427", key = "secret0427")
print(password)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table table0212(id int)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into table0212(id) values(1)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table0212

# COMMAND ----------

# MAGIC %sh 
# MAGIC nc -vz adb-3885924896301357.1.databricks.azure.cn 443

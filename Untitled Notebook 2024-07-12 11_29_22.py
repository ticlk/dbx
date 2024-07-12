# Databricks notebook source
# MAGIC %sql
# MAGIC create table table0712( id int, fname varchar(20))

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into table0712(id, fname) values(1,'test')

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into table0712(id, fname) values(7,'test 07')

# COMMAND ----------

# MAGIC %sql
# MAGIC update table0712 set fname = 'update 1' where id = 4

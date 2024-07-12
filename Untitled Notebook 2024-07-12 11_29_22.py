# Databricks notebook source
# MAGIC %sql
# MAGIC create table table0712( id int, fname varchar(20))

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into table0712(id, fname) values(1,'test')

# MAGIC %sql
# MAGIC insert into table0712(id, fname) values(2,'test 2')

# MAGIC %sql
# MAGIC update table0712 set fname = 'update 2' where id =2


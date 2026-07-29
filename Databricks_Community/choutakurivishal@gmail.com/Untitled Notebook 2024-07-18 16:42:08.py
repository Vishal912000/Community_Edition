# Databricks notebook source
print ("abc")

# COMMAND ----------

import json

# COMMAND ----------

dbutils.widgets.text(name="args",defaultValue="",label="args")
args=dbutils.widgets.get("args")
if args:
    args=json.loads(args)
    catalog_name=args["catalog_name"]
    database_name=args["database_name"]
else:
    catalog_name=None
    database_name=None


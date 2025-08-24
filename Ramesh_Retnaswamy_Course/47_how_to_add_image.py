# Databricks notebook source
#To add a image ,we need to upload the image in the dbfs or any file storage system  and collect the url 
#Then Just change the cell format to using %md magic command (markdown) and you will see a image option
# add the url in place of path

# COMMAND ----------

#below you can see i have added

# To get the url in dbfs

#we need to add https://community.cloud.databricks.com/files at the the start of the file_path from dbfs

#this is the acutual dbfs file path --->dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/MERGE_image.jpg

#so,I am adding url and removing dbfs and Filestore

#https://community.cloud.databricks.com/files/shared_uploads/choutakurivishal@gmail.com/MERGE_image.jpg



# COMMAND ----------

# MAGIC %md
# MAGIC ![Merge_image](https://community.cloud.databricks.com/files/shared_uploads/choutakurivishal@gmail.com/MERGE_image.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ![](https://community.cloud.databricks.com/files/shared_uploads/choutakurivishal@gmail.com/MERGE_Statement_in_SQL_Example.jpg)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md

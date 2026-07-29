# Databricks notebook source
# MAGIC %md #merge
# MAGIC merge command combines the insert,update and delete commands in a single query
# MAGIC
# MAGIC merge command compare data between two tables identifying as source and target based on specific column
# MAGIC
# MAGIC Then ,It performs actions like insert,update and delete on target table
# MAGIC
# MAGIC
# MAGIC
# MAGIC ![Merge Image](https://community.cloud.databricks.com/files/shared_uploads/choutakurivishal@gmail.com/MERGE_image.jpg)
# MAGIC
# MAGIC Syntax:
# MAGIC
# MAGIC merge into table_name as target
# MAGIC
# MAGIC using new_table_name as source
# MAGIC
# MAGIC on target.col_name=source.new_col_name
# MAGIC
# MAGIC when matched then 
# MAGIC
# MAGIC update set col_name='value'
# MAGIC
# MAGIC when not matched then 
# MAGIC
# MAGIC insert (col1,col2,col3)
# MAGIC values(value1,value2,value3)
# MAGIC

# COMMAND ----------

#so here  we can see two tables in the image ,one is available product list and new updated list 
# We need to 
# 1)update the price of coffee ,
# 2)delete biscuit and 
# 3)insert chips  

#So we can use merge

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,IntegerType,StringType,DoubleType

# COMMAND ----------

data=[(101,"Tea",10.50),(102,"Coffee",15.00),(103,"Biscuit",20.00)]

schema=StructType(fields=[StructField("id",IntegerType(),nullable=True),
                           StructField("name",StringType(),nullable=True),
                           StructField("price",DoubleType(),nullable=True)
                           ])


# COMMAND ----------

old_list=spark.createDataFrame(data,schema)
display(old_list)

# COMMAND ----------

new_data=[(101,"Tea",10.00),(102,"Coffee",25.00),(103,"Chips",22.00)]
new_schema=StructType(fields=[StructField("id",IntegerType(),nullable=True),
                          StructField("name",StringType(),nullable=True),
                           StructField("price",DoubleType(),nullable=True)
                           ])

# COMMAND ----------

new_list=spark.createDataFrame(new_data,new_schema)
display(old_list)
display(new_list)

# COMMAND ----------

old_list.createOrReplaceTempView("old_list_table")
new_list.createOrReplaceTempView("new_list_table")

#Here we are creating temporary table but merge option is not getting supported in this temporary tables because they don't support acid properties

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table new_list_delta_table
# MAGIC
# MAGIC --using drop  for deleting unwanted tables

# COMMAND ----------

old_list.write.format("delta").mode("overwrite").saveAsTable(f"old_list_delta_table")
new_list.write.format("delta").mode("overwrite").saveAsTable(f"new_list_delta_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC merge into old_list_delta_table  as target
# MAGIC using new_list_delta_table as source
# MAGIC on target.id=source.id
# MAGIC
# MAGIC when matched 
# MAGIC and target.name<>source.name
# MAGIC or target.price<>source.price
# MAGIC
# MAGIC then update
# MAGIC set target.name = source.name,
# MAGIC target.price=source.price
# MAGIC
# MAGIC when not matched by target
# MAGIC then insert (id,name,price)
# MAGIC values(source.id,source.name,source.price)
# MAGIC
# MAGIC when not matched by source
# MAGIC then delete
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Specify the target Delta table (old_list_delta_table)  as 'target'
# MAGIC merge into old_list_delta_table as target 
# MAGIC
# MAGIC -- Specify the source Delta table (new_list_delta_table)  as 'source'
# MAGIC using new_list_delta_table as source 
# MAGIC on target.id = source.id 
# MAGIC
# MAGIC -- If rows match (based on the ON condition) and either the 'name' or 'price' differs, update the target table
# MAGIC when matched 
# MAGIC     and target.name <> source.name  -- Check if the 'name' value is different
# MAGIC     or target.price <> source.price -- Check if the 'price' value is different
# MAGIC
# MAGIC -- Perform the update operation: set 'name' and 'price' in target to match those in source
# MAGIC then update
# MAGIC     set target.name = source.name,  -- Update the 'name' column in target
# MAGIC         target.price = source.price -- Update the 'price' column in target
# MAGIC
# MAGIC -- If a row exists in the source table but not in the target table, insert it into the target
# MAGIC when not matched by target
# MAGIC then insert (id, name, price)       -- Columns to insert into target table
# MAGIC      values (source.id, source.name, source.price) -- Corresponding values from source table
# MAGIC
# MAGIC -- If a row exists in the target table but not in the source table, delete it from the target
# MAGIC when not matched by source
# MAGIC then delete  -- Delete rows in target that don't have a corresponding match in source
# MAGIC
# MAGIC --If not understanding please check how to do (only insert and update) or (only insert and delete)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.old_list_delta_table

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE into old_list_delta_table AS TARGET
# MAGIC     USING new_list_delta_table AS SOURCE 
# MAGIC
# MAGIC     /* 1. Performing the UPDATE operation */
# MAGIC
# MAGIC     /* If the P_ID is same, 
# MAGIC        check for change in P_NAME or P_PRICE */
# MAGIC     ON (TARGET.id = SOURCE.id)
# MAGIC     WHEN MATCHED 
# MAGIC          AND TARGET.name <> SOURCE.name
# MAGIC          OR TARGET.price <> SOURCE.price
# MAGIC
# MAGIC     /* Update the records in TARGET */
# MAGIC     THEN UPDATE 
# MAGIC          SET TARGET.name = SOURCE.name,
# MAGIC          TARGET.price = SOURCE.price
# MAGIC      
# MAGIC     /* 2. Performing the INSERT operation */
# MAGIC
# MAGIC     /* When no records are matched with TARGET table 
# MAGIC        Then insert the records in the target table */
# MAGIC     WHEN NOT MATCHED BY TARGET 
# MAGIC     THEN INSERT (id, name, price)          
# MAGIC          VALUES (SOURCE.id, SOURCE.name, SOURCE.price)
# MAGIC
# MAGIC     WHEN NOT MATCHED BY SOURCE 
# MAGIC     THEN DELETE

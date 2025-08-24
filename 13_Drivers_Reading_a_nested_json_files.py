# Databricks notebook source
# MAGIC %md 
# MAGIC A nested JSON file is a JSON (JavaScript Object Notation) file where one or more values 
# MAGIC are JSON objects or arrays themselves. 
# MAGIC
# MAGIC This means that the structure of the data is hierarchical, with objects or arrays inside
# MAGIC
# MAGIC other objects or arrays,
# MAGIC
# MAGIC creating a "nested" structure
# MAGIC
# MAGIC ***For reference check the name column in drivers json

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,DateType

# COMMAND ----------

drivers_schema=StructType(fields=[StructField('code',IntegerType(),nullable=True),
                                  StructField('dob',DateType(),True),
                                  StructField('driverId',IntegerType(),True),
                                  StructField('driverRef',StringType(),True),
                                  StructField('forename',StringType(),True),
                                  StructField('surname',StringType(),True)
                                  ])

drivers_df=spark.read.format('json').load('dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/drivers.json',header=True,schema=drivers_schema)
display(drivers_df)

# If we read like this it will show columns forename and surname as null

# COMMAND ----------

# To read a nested json file we need to define the schema of the inner array/list and then define the outer array/list and we need to add schema name of innerschema as a datatype in outer schema 

# So here in our case we have name as forename and surname ,so we need to define inner schema(name schema) and then outer schema(our schema) 

name_schema=StructType(fields=[StructField('forename',StringType(),True),
                               StructField('surname',StringType(),True)
                               ])

drivers_schema=StructType(fields=[StructField('code',StringType(),True),
                                  StructField('dob',DateType(),True),
                                  StructField('driverId',IntegerType(),True),
                                  StructField('driverRef',StringType(),True),
                                  StructField('name',name_schema,True),
                                  StructField('nationality',StringType(),True),
                                  StructField('number',StringType(),True),
                                  StructField('url',StringType(),True)
                                    ])



# COMMAND ----------

drivers_df=spark.read.format('json').load('dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/drivers-1.json',header=True,schema=drivers_schema)
display(drivers_df)

# COMMAND ----------

from pyspark.sql.functions import col,lit,concat,current_timestamp

# COMMAND ----------

drivers_df_renamed=drivers_df.withColumnRenamed('driverId','driver_id')\
                             .withColumnRenamed('driverRef','driver_ref')\
                             .withColumn('ingestion_date',current_timestamp())\
                             .withColumn('name',concat( col('name.forename'),lit(' '),col('name.surname') ) )

display(drivers_df_renamed)

# COMMAND ----------

# dropping the unwanted columns as we already had a name field it got replaced with new concatenated one ,now we need to drop url column
drivers_df_drop=drivers_df_renamed.drop("url")

# COMMAND ----------

drivers_df_drop.write.format("parquet").mode("overwrite").save("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/drivers_final")

# COMMAND ----------

df_samp=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/drivers_final",header=True)
display(df_samp)
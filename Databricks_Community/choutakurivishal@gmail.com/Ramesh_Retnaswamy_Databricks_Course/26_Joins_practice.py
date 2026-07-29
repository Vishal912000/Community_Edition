# Databricks notebook source
#Here we are going to join all dfs to get some related date
#sources : 1) races
#          2) circuits
#          3) drivers
#          4) constructors
#          5) results

# COMMAND ----------

races_df=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/races_final",header=True)
circuits_df=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/circuits_final",header=True)
constructor_df=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/constructor_final",header=True)
drivers_df=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/drivers_final",header=True)
results_df=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/results_final",header=True)
pitstops_df=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/pit_stops_final",header=True)


# COMMAND ----------

#Renaming columns in dataframes to avoid same name in another dataframes
races_df2=races_df.withColumnRenamed("circuit_id","race_circuit_id")\
                  .withColumnRenamed("name","race_name")\
                  .withColumnRenamed("ingestion_time","races_ingestion_time")

circuits_df2=circuits_df.withColumnRenamed("name","circuit_name")\
                            .withColumnRenamed("location","circuit_location")\
                            .withColumnRenamed("ingestion_date","circuits_ingestion_time")

constructor_df2=constructor_df.withColumnRenamed("name","constructor_name")\
                              .withColumnRenamed("ingestion_time","constructor_ingestion_time")



drivers_df2=drivers_df.withColumnRenamed("name","driver_name")\
                      .withColumnRenamed("nationality","driver_nationality")\
                      .withColumnRenamed("number","driver_number")\
                      .withColumnRenamed("ingestion_date","drivers_ingestion_time")

results_df2=results_df.withColumnRenamed("constructor_id","results_constructor_id")\
                      .withColumnRenamed("driver_id","results_driver_id")\
                      .withColumnRenamed("race_id","results_race_id")\
                      .withColumnRenamed("ingestion_time","results_ingestion_time")


pitstops_df2=pitstops_df.withColumnRenamed("race_id","pitstops_race_id")\
                        .withColumnRenamed("driver_id","pitstops_driver_id")\
                        .withColumnRenamed("ingestion_time","pitstops_ingestion_time")\
                        .withColumnRenamed("time","pitstops_time")



# COMMAND ----------

races_circuits=races_df2.join(circuits_df2,races_df2.race_circuit_id==circuits_df2.circuit_id,"inner")
#display(races_circuits)

# COMMAND ----------

constructor_results_drivers=constructor_df2.join(results_df2,constructor_df2.constructor_id==results_df2.results_constructor_id)\
                                            .join(drivers_df2,constructor_results_drivers.results_driver_id==drivers_df2.driver_id)

# COMMAND ----------

final=races_circuits.join(constructor_results_drivers,races_circuits.race_id==constructor_results_drivers.results_race_id)
#display(final)

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp

# COMMAND ----------

final_selected=final.select(col("year").alias("race_year"),
                            col("race_name"),
                            col("circuit_location"),
                            col("driver_name"),
                            col("driver_number"),
                            col("constructor_name").alias("team"),
                            col("grid"),
                            col("fastest_lap"),
                            col("time").alias("race_time"),
                            col("points")).distinct()


# COMMAND ----------

#display(final_selected)

# COMMAND ----------

final_2020=final_selected.filter((final_selected.race_year==2020) & (final_selected.race_name=='Abu Dhabi Grand Prix')).orderBy(final_selected.points.desc())
display(final_2020)

# COMMAND ----------

final_selected.write.format("parquet").mode("overwrite").save("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/races_result")

# COMMAND ----------

df=spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/choutakurivishal@gmail.com/races_result",header=True)
display(df)

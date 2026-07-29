# Databricks notebook source
# MAGIC %md
# MAGIC #ACID Properties
# MAGIC #A-ATOMICITY
# MAGIC #C-CONSISTENCY
# MAGIC #I-ISOLATION
# MAGIC #D-DURABILITY
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #1-Atomicity (All or Nothing)
# MAGIC A transaction is atomic if it executes completely or not at all,If one part fails entire transaction is rolled back
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC begin transaction 
# MAGIC
# MAGIC update fwk.report_orchestartion 
# MAGIC set email_id ="v@gmail.com"
# MAGIC where id=1
# MAGIC
# MAGIC update fwk.report_orchestration
# MAGIC set email_id ="c@gmail.com"
# MAGIC where id=2
# MAGIC
# MAGIC commit;
# MAGIC
# MAGIC --In this transaction ,If the 2nd update fails ,1st one is rolled back to prevent corruption.
# MAGIC --This is called as Atomicity

# COMMAND ----------

# MAGIC %md
# MAGIC #Consistency (Valid State before and after transaction):-
# MAGIC
# MAGIC Ensures database enforces schema consistency ,so that only valid data enters the table
# MAGIC
# MAGIC It prevents corruption of data 
# MAGIC
# MAGIC Ex:-If a column expects a INT value ,but recieves a string ,The query fails and prevents corruption of the data
# MAGIC
# MAGIC This is called consistency

# COMMAND ----------

# MAGIC %md
# MAGIC #Isolation(NO interfernece b/w multiple transactions):-
# MAGIC
# MAGIC Ensures concurrent transactions operate without any conflict
# MAGIC
# MAGIC Ex:-If two users are updating the table at the same time ,It should detect the conflict and prevent one transaction from overwriting another 
# MAGIC
# MAGIC This is called as Isolation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Transaction 1
# MAGIC BEGIN;
# MAGIC UPDATE delta_table SET name='Alice' WHERE id=1;
# MAGIC
# MAGIC -- Transaction 2 (runs at the same time)
# MAGIC BEGIN;
# MAGIC UPDATE delta_table SET name='Bob' WHERE id=1;
# MAGIC COMMIT;  -- This may fail due to conflict handling in Delta Lake
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Durability:
# MAGIC
# MAGIC Ensures thatonce a transaction is committed,It is permanently saved in the database,even in case of a system failure
# MAGIC
# MAGIC Data should remain intact even after a power failure or crash

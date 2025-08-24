# Databricks notebook source
# MAGIC %md
# MAGIC Sure! Here's a **detailed explanation** of **Slowly Changing Dimensions (SCD)**, including what they are, why they matter, the different types, and practical examples:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 What are Slowly Changing Dimensions (SCD)?
# MAGIC
# MAGIC In a **data warehouse**, **dimensions** (like customer, employee, product) contain **descriptive attributes**. Over time, these attributes may change — e.g., a customer moves to a new address or changes their name.
# MAGIC
# MAGIC To maintain **data accuracy** and **historical context**, we use **Slowly Changing Dimensions (SCDs)** to decide **how to handle these changes**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧩 Why SCDs Matter
# MAGIC
# MAGIC SCDs help answer key business questions:
# MAGIC
# MAGIC * "What was the customer's address when they placed the order in 2022?"
# MAGIC * "How many products were sold while they were in the 'premium' category?"
# MAGIC
# MAGIC Without tracking changes properly, your data might reflect **only the current state**, not the **state at the time of the event** — leading to **incorrect insights**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧾 Types of Slowly Changing Dimensions
# MAGIC
# MAGIC | **Type** | **Name**                    | **Description**                                                           | **When to Use**                                     |
# MAGIC | -------- | --------------------------- | ------------------------------------------------------------------------- | --------------------------------------------------- |
# MAGIC | Type 0   | **Fixed / Retain Original** | Do not allow changes to the attribute.                                    | Immutable facts (e.g., date of birth)               |
# MAGIC | Type 1   | **Overwrite**               | Replace old data with new (no history preserved).                         | Correcting errors where history isn't needed        |
# MAGIC | Type 2   | **Add Row (Track History)** | Create a new row for each change, with versioning and effective dates.    | When full historical tracking is required           |
# MAGIC | Type 3   | **Add Column**              | Add a new column to store previous value(s).                              | Track limited history (e.g., previous vs current)   |
# MAGIC | Type 4   | **History Table**           | Maintain current row in dimension, but track history in a separate table. (Store full history in a separate table) | When separating history from current state is ideal |
# MAGIC | Type 6   | **Hybrid (1+2+3)**          | Combines Type 1, 2, and 3 for optimized tracking and querying.            | Advanced use cases needing both history and current |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Example: Customer Address Change
# MAGIC
# MAGIC ### Scenario:
# MAGIC
# MAGIC A customer changes their address from "New York" to "San Francisco".
# MAGIC
# MAGIC ### Type 1 (Overwrite):
# MAGIC
# MAGIC | Customer\_ID | Name  | Address       |
# MAGIC | ------------ | ----- | ------------- |
# MAGIC | 101          | Alice | San Francisco |
# MAGIC
# MAGIC * Old address is lost.
# MAGIC * Useful for correcting wrong info.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Type 2 (Add Row, Track History):
# MAGIC
# MAGIC | Customer\_ID | Name  | Address       | Start\_Date | End\_Date  | Current\_Flag |
# MAGIC | ------------ | ----- | ------------- | ----------- | ---------- | ------------- |
# MAGIC | 101          | Alice | New York      | 2022-01-01  | 2023-05-01 | N             |
# MAGIC | 101          | Alice | San Francisco | 2023-05-02  | NULL       | Y             |
# MAGIC
# MAGIC * Tracks full history.
# MAGIC * Needed for time-based reporting.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Type 3 (Add Column):
# MAGIC
# MAGIC | Customer\_ID | Name  | Current\_Address | Previous\_Address |
# MAGIC | ------------ | ----- | ---------------- | ----------------- |
# MAGIC | 101          | Alice | San Francisco    | New York          |
# MAGIC
# MAGIC * Only one previous state tracked.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Type 4 (History Table):
# MAGIC
# MAGIC #### Current Table:
# MAGIC
# MAGIC | Customer\_ID | Name  | Current\_Address |
# MAGIC | ------------ | ----- | ---------------- |
# MAGIC | 101          | Alice | San Francisco    |
# MAGIC
# MAGIC #### History Table:
# MAGIC
# MAGIC | Customer\_ID | Address       | Change\_Date |
# MAGIC | ------------ | ------------- | ------------ |
# MAGIC | 101          | New York      | 2022-01-01   |
# MAGIC | 101          | San Francisco | 2023-05-01   |
# MAGIC
# MAGIC * Clean separation between current and historical data.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Type 6 (Hybrid):
# MAGIC
# MAGIC | Customer\_ID | Name  | Current\_Address | Previous\_Address | Start\_Date | End\_Date | Current\_Flag |
# MAGIC | ------------ | ----- | ---------------- | ----------------- | ----------- | --------- | ------------- |
# MAGIC | 101          | Alice | San Francisco    | New York          | 2023-05-02  | NULL      | Y             |
# MAGIC
# MAGIC * Combines:
# MAGIC
# MAGIC   * Type 1: Overwrites current column
# MAGIC   * Type 2: Adds new row
# MAGIC   * Type 3: Tracks previous value in column
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ SCD Implementation in PySpark (Type 2 — Track History)
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import lit, current_date
# MAGIC
# MAGIC # Example: New data
# MAGIC new_df = spark.createDataFrame([
# MAGIC     (101, "Alice", "San Francisco")
# MAGIC ], ["Customer_ID", "Name", "Address"])
# MAGIC
# MAGIC # Existing dimension table
# MAGIC old_df = spark.read.table("dim_customer")
# MAGIC
# MAGIC # Detect changes
# MAGIC join_cond = ["Customer_ID"]
# MAGIC changed_df = new_df.alias("new").join(old_df.alias("old"), join_cond) \
# MAGIC     .filter("new.Address != old.Address") \
# MAGIC     .select("new.*")
# MAGIC
# MAGIC # Mark old record as inactive
# MAGIC expired_df = old_df.join(changed_df, join_cond) \
# MAGIC     .withColumn("End_Date", current_date()) \
# MAGIC     .withColumn("Current_Flag", lit("N"))
# MAGIC
# MAGIC # Insert new record with updated info
# MAGIC new_version_df = changed_df \
# MAGIC     .withColumn("Start_Date", current_date()) \
# MAGIC     .withColumn("End_Date", lit(None).cast("date")) \
# MAGIC     .withColumn("Current_Flag", lit("Y"))
# MAGIC
# MAGIC # Union to get final dimension
# MAGIC final_df = old_df.subtract(expired_df).union(new_version_df)
# MAGIC
# MAGIC # Write back to the dimension table
# MAGIC final_df.write.mode("overwrite").saveAsTable("dim_customer")
# MAGIC ```
# MAGIC
# MAGIC ---

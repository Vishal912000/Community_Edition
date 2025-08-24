# Databricks notebook source
# MAGIC %md
# MAGIC PySpark and Spark SQL have roughly the same performance.
# MAGIC This is because both are translated into the same underlying execution plan by Apache Spark's Catalyst optimizer.

# COMMAND ----------

# MAGIC %md
# MAGIC Here is a **side-by-side comparison table** of **PySpark** and **Spark SQL** in the context of Databricks:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔍 **PySpark vs Spark SQL Comparison**
# MAGIC
# MAGIC | Aspect                         | **PySpark**                                                        | **Spark SQL**                                                   |
# MAGIC | ------------------------------ | ------------------------------------------------------------------ | --------------------------------------------------------------- |
# MAGIC | **Language**                   | Python                                                             | SQL                                                             |
# MAGIC | **Syntax Style**               | `df.select("name").filter(df.age > 30)`                            | `SELECT name FROM people WHERE age > 30`                        |
# MAGIC | **Execution Context**          | `%python` cells or default Python notebook mode                    | `%sql` cells or `spark.sql("...")` inside Python                |
# MAGIC | **Ease of Use**                | Better for programmers familiar with Python                        | Easier for analysts or SQL-savvy users                          |
# MAGIC | **Data Source Access**         | Through Spark APIs like `spark.read`                               | Requires a temp/view created from a DataFrame                   |
# MAGIC | **Temp View Required?**        | Yes, if you want to query using Spark SQL                          | No, if already querying a known table/view                      |
# MAGIC | **Use of UDFs**                | Supports Python UDFs (slower) and Spark SQL UDFs                   | Supports only SQL-compatible UDFs (faster if JVM-based)         |
# MAGIC | **Performance**                | Similar performance **unless using Python UDFs**, which are slower | Slightly better for simple queries (due to easier optimization) |
# MAGIC | **Control Flow**               | Full support (loops, conditionals, functions, etc.)                | Limited (no loops/if-else logic in SQL directly)                |
# MAGIC | **Integration with Libraries** | Easy to integrate with Python libraries like Pandas, NumPy, etc.   | Not applicable                                                  |
# MAGIC | **Debugging**                  | Easier with IDE tools or Python stack traces                       | Harder to debug complex queries                                 |
# MAGIC | **Code Reusability**           | High (can use functions, classes, modules)                         | Lower (unless using stored SQL templates)                       |
# MAGIC | **Best For**                   | Complex ETL, data wrangling, dynamic pipelines                     | Fast prototyping, ad hoc analysis, dashboard queries            |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧠 Example Comparison
# MAGIC
# MAGIC | Task        | **PySpark**                                    | **Spark SQL**                                      |
# MAGIC | ----------- | ---------------------------------------------- | -------------------------------------------------- |
# MAGIC | Load data   | `df = spark.read.csv("file.csv", header=True)` | `%sql\nCREATE TABLE my_table AS SELECT * FROM ...` |
# MAGIC | Filter rows | `df.filter(df.age > 25)`                       | `SELECT * FROM my_table WHERE age > 25`            |
# MAGIC | Add column  | `df.withColumn("age2", df.age + 5)`            | `SELECT *, age + 5 AS age2 FROM my_table`          |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Let me know if you'd like this exported to CSV, Markdown, or formatted for a slide deck.
# MAGIC
# Databricks notebook source
# # list(("a", "aa", 1), ("b", "bb", 2), ("c", "cc", 3))
# #
# # output:
# # list(("a", 1), ("b", 4), ("c", 9))


s = "aaabbbbcc"
# o/p = "3a4b2c"


# COMMAND ----------

# MAGIC %sql
# MAGIC -- your code goes here
# MAGIC CREATE TABLE Employee_Salary_History (
# MAGIC     employee_id INT,
# MAGIC     change_date DATE,
# MAGIC     salary INT
# MAGIC );
# MAGIC
# MAGIC INSERT INTO Employee_Salary_History (employee_id, change_date, salary) VALUES
# MAGIC (1, '2023-01-01', 50000),
# MAGIC (1, '2023-06-01', 55000),
# MAGIC (1, '2024-01-01', 60000),
# MAGIC (2, '2023-03-15', 40000),
# MAGIC (2, '2023-12-01', 45000),
# MAGIC (3, '2023-07-01', 70000);
# MAGIC
# MAGIC
# MAGIC select *,salary-previous_salary as salary_difference from (select *,lag(salary) over(partition by employee_id order by change_date)
# MAGIC as previous_salary 
# MAGIC from Employee_Salary_History)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- your code goes here
# MAGIC CREATE TABLE Employee_Salary_History (
# MAGIC     employee_id INT,
# MAGIC     change_date DATE,
# MAGIC     salary INT
# MAGIC );
# MAGIC
# MAGIC INSERT INTO Employee_Salary_History (employee_id, change_date, salary) VALUES
# MAGIC (1, '2023-01-01', 50000),
# MAGIC (1, '2023-06-01', 55000),
# MAGIC (1, '2024-01-01', 60000),
# MAGIC (2, '2023-03-15', 40000),
# MAGIC (2, '2023-12-01', 45000),
# MAGIC (3, '2023-07-01', 70000);
# MAGIC
# MAGIC
# MAGIC select *,salary-previous_salary as salary_difference from (select *,lag(salary) over(partition by employee_id order by change_date)
# MAGIC as previous_salary 
# MAGIC from Employee_Salary_History)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE Sales (
# MAGIC     sale_date DATE,
# MAGIC     fruit VARCHAR(20),
# MAGIC     sold_num INT
# MAGIC );
# MAGIC
# MAGIC INSERT INTO Sales (sale_date, fruit, sold_num) VALUES
# MAGIC ('2020-05-01', 'apples', 10),
# MAGIC ('2020-05-01', 'oranges', 8),
# MAGIC ('2020-05-02', 'apples', 15),
# MAGIC ('2020-05-02', 'oranges', 15),
# MAGIC ('2020-05-03', 'apples', 20),
# MAGIC ('2020-05-03', 'oranges', 0),
# MAGIC ('2020-05-04', 'apples', 15),
# MAGIC ('2020-05-04', 'oranges', 16);
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC
# MAGIC with apples as (select *, lag(sold_num) over
# MAGIC  (partition by sale_date order by sale_date)
# MAGIC  as apples_sold from Sales)
# MAGIC  
# MAGIC select sale_date,apples_sold-sold_num as diff  from apples
# MAGIC
# MAGIC --need to remove null values

# COMMAND ----------

mydata=" vishal is a great person ,he will make very great things that will make india a super power"

Please count occurence of each word using pyspark

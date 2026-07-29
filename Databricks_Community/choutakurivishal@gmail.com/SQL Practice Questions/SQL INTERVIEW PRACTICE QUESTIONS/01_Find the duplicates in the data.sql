-- Databricks notebook source
-- MAGIC %run /Workspace/Repos/choutakurivishal@gmail.com/Community_Edition/Databricks_Community/choutakurivishal@gmail.com/Tables/Employee_with_duplicates

-- COMMAND ----------

--Here we are using count to find the duplicates count will give the no. of occurences in the table and we are grouping them by emp_id 
-- and then filterning them using having inside group by clause

-- 1. Write a SQL query to group the data by `emp_id` and count the number of times each ID appears.

-- 2. Use a `HAVING` clause to filter for `emp_id`s that appear more than once.


-- COMMAND ----------

select emp_id,count(*)  as count_of_employees from employees_df
group by emp_id
having count_of_employees >1

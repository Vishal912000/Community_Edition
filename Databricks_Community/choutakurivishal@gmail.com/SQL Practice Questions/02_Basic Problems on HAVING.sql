-- Databricks notebook source
-- MAGIC %run /Users/choutakurivishal@gmail.com/Tables/Employee_table

-- COMMAND ----------

select * from employee_df

-- COMMAND ----------

--Problem 1.Find departments with average salary greater than 75,000

select dept,avg(salary) from employee_df
group by dept
having avg(salary)>75000

-- COMMAND ----------

--Problem2:Get departments having more than 3 employees

select dept,count(*) as count
from employee_df
group by dept 
having count>3

-- COMMAND ----------

--Problem 3. Find departments where total salary exceeds 200,000

select dept,sum(salary) as salary_sum
from employee_df
group by dept 
having salary_sum>200000

-- COMMAND ----------

--Problem 5. Departments with average employee age between 30 and 40

select dept,avg(age) as avg_age
from employee_df
group by dept
having avg_age between 30 and 40

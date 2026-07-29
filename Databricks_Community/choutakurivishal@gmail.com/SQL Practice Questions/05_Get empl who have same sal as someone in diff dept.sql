-- Databricks notebook source
-- MAGIC %run /Users/choutakurivishal@gmail.com/Tables/Employee_table

-- COMMAND ----------

select * from employee_df

-- COMMAND ----------

1.Use a self-join on the employees table to compare each employee with others.

2.Match rows where the salary is the same.

3.Make sure the departments are different.

4.Show the details of the employees who meet these two conditions.

-- COMMAND ----------

select e1.* from employee_df e1
join employee_df e2
on e1.salary=e2.salary
and e1.dept!=e2.dept

-- COMMAND ----------



-- Databricks notebook source
-- MAGIC %run /Users/choutakurivishal@gmail.com/Tables/Employee_with_duplicates

-- COMMAND ----------

--1sd method 
select * from (select * ,dense_rank(salary) over(order by salary desc) as rank from employees_df)
where rank=2

-- COMMAND ----------

--2nd method
select * from employees_df
order by salary desc
limit 1 offset 1

-- COMMAND ----------

--3rd method
select max(salary) as 2nd_high from employees_df
where salary !=(select max(salary) from employees_df 
                 )

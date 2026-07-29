-- Databricks notebook source
-- MAGIC %run /Users/choutakurivishal@gmail.com/Tables/Departments

-- COMMAND ----------

-- select * from employees_df 
-- where emp_id in (select distinct manager_id from employees_df)


with managers as (select distinct manager_id from employees_df)

select e.* from employees_df  e
join managers m
on e.emp_id=m.manager_id

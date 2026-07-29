-- Databricks notebook source
-- MAGIC %run /Users/choutakurivishal@gmail.com/Tables/Departments

-- COMMAND ----------

select * from departments_df 

-- COMMAND ----------

--  1. **Use a left join** so that all employees stay from employees , even if they don’t have a department.  
-- 2. **Keep only the employees** who didn’t match any department (where `dept_id` in departments is missing).  
-- 3. **Show only the employee details** in the final result.


-- 1.Join employees and departments table using left join on e.dept_id=d.dept_id
-- 2. 


-- COMMAND ----------

select * from employees_df e
left join departments_df d on 
e.dept_id=d.dept_id

-- COMMAND ----------

--1st method :- select * from employees_df where dept not in (select dept from departments_df)

select e.*  from employees_df e
left join departments_df d on 
e.dept_id=d.dept_id
where d.dept_id is null

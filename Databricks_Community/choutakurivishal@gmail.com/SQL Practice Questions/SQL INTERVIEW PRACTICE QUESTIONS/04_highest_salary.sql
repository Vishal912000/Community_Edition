-- Databricks notebook source
-- MAGIC %run /Users/choutakurivishal@gmail.com/Tables/Employee_with_duplicates

-- COMMAND ----------

select distinct * from employees_df

-- COMMAND ----------

--1st method 
select * from employees_df 
where salary =(select max(salary)  from employees_df)



-- COMMAND ----------

--2nd method
select * from employees_df 
order by salary desc
limit 1

-- COMMAND ----------

--3rd method
with max_salary as (select max(salary) as salary from employees_df)

select * from employees_df e
join max_salary m
on e.salary=m.salary



-- COMMAND ----------

--4th method
select * from (select * ,dense_rank(salary) over(order by salary desc) as rank from employees_df)
where rank=1

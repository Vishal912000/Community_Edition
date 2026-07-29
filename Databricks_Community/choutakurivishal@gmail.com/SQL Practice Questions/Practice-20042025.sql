-- Databricks notebook source
-- MAGIC %run /Users/choutakurivishal@gmail.com/Tables/Employee_with_duplicates

-- COMMAND ----------

-- 04_highest_salary

-- --1st method
-- select * from employees_df
-- order by salary desc
-- limit 1

-- --2nd method 
-- select * from employees_df where salary=(select max(salary) from employees_df) 

-- --3rd method 
-- with max_sal as (select max(salary) as salary from employees_df)

-- select e.* from employees_df e
-- join max_sal m
-- on e.salary=m.salary

-- --4th method 
-- select * from (select *,dense_rank() over (order by salary desc)  as rank from employees_df)
-- where rank=1






-- COMMAND ----------

-- 05_2nd highest_salary

-- --1st method
-- select * 
-- from employees_df 
-- where salary=(select max(salary) 
--               from employees_df
--               where salary !=(select max(salary) 
--                               from employees_df))


-- --2nd method
-- select *  from (select * ,dense_rank() over (order by salary desc) as d_rank from employees_df )
-- where d_rank=2


--3rd method
with d_rank as (select  *,dense_rank() over(order by salary desc) as d_rank from employees_df)

select distinct e.* from employees_df e
join d_rank d
on e.emp_id=d.emp_id
where d_rank=2



-- COMMAND ----------

-- 06_highest_salary and 2nd highest_sal_in each department


--1st method
with high_sal as (select dept_id,max(salary) as salary
from employees_df
group by dept_id)

select e.* from employees_df e
join high_sal h
on e.dept_id=h.dept_id
and e.salary=h.salary


-- --2nd method 
-- select * from (select * ,dense_rank() over(partition by dept_id order by salary desc) as d_rank from employees_df)
-- where d_rank=1



-- COMMAND ----------

-- select * from (select *,dense_rank() over(partition by dept order by salary desc) as d_rank from employees_df)
-- where d_rank=1

with d_rank(select * ,dense_rank() over(partition by dept order by salary desc ) as rank 
from employees_df) 

select e.* from employees_df e
join d_rank d
on e.emp_id=d.emp_id
where rank=1

-- COMMAND ----------

--08_Find_emp_whose_sal>mgr_sal_incomplete

select * from employees_df e1
join employees_df e2
on e1.manager_id=e2.emp_id
where e1.salary>e2.salary

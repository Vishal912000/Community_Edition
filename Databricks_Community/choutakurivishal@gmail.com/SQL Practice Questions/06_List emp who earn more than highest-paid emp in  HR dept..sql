-- Databricks notebook source
-- MAGIC %run /Users/choutakurivishal@gmail.com/Tables/Employee_table

-- COMMAND ----------

--List employees who earn more than the highest-paid employee in the HR department.


-- COMMAND ----------

select * from employee_df

-- COMMAND ----------

with hr_high 
(select max(salary) as high_sal
from employee_df
where dept='HR'
)

select * from employee_df  e
join hr_high h
on e.salary>h.high_sal



-- COMMAND ----------

select max(salary) as high_sal
from employee_df
where



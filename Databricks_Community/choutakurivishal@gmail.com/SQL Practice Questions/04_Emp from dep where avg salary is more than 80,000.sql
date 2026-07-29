-- Databricks notebook source
-- MAGIC %run /Users/choutakurivishal@gmail.com/Tables/Employee_table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(employee_df)

-- COMMAND ----------

To find employees from departments where the average salary is more than 80,000
 
-- 1. **Calculate the average salary** of employees in each department.  
-- 2. **Select only those departments** where the average salary is **greater than 80,000**.  
-- 3. **Find all employees** who belong to those selected departments.  
-- 4. **List those employees** in the final output.  

-- COMMAND ----------

with dept_sal (select avg(salary) as avg_salary,dept from employee_df
group by dept
having avg_salary>80000)

select e.* from employee_df e
join dept_sal  d
on e.dept=d.dept


--e.* means you're selecting only the columns from employee_df.

-- Databricks notebook source
-- MAGIC %run /Users/choutakurivishal@gmail.com/Tables/Employee_table

-- COMMAND ----------

-- List employees who earn more than the highest-paid employee in the HR department

-- COMMAND ----------

-- 1. Find the **highest salary** in the HR department.  
-- 2. Save that salary in a **temporary table** called `hr_high`.  
-- 3. Look at **all employees** in the company.  
-- 4. Compare each employee’s salary with the HR highest salary.  
-- 5. **Pick only those** who earn **more than** the HR highest salary.  

-- COMMAND ----------

with hr_dep_max_sal (select max(salary) as max_sal  from employee_df
where dept='HR')

select * from employee_df e
join hr_dep_max_sal h
on e.salary>h.max_sal




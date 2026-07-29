-- Databricks notebook source
-- MAGIC %run /Users/choutakurivishal@gmail.com/Tables/Employee_table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The query says to find all the employees who earn more than average salary of their department
-- MAGIC
-- MAGIC so,It means to find salary of the employees who are earning salaries than their department
-- MAGIC
-- MAGIC Let's take one example of the department (finance) and apply average to that department
-- MAGIC
-- MAGIC Finance ->Bob-80000,Frank-90000,Mike-85000,Quinn-81000
-- MAGIC
-- MAGIC Average is 336000/4=84000
-- MAGIC
-- MAGIC So  we need to get for finance department whose salaries are greater than 84000
-- MAGIC
-- MAGIC so our answer would be Frank and Mike
-- MAGIC
-- MAGIC Now we write a query for this

-- COMMAND ----------

--1st we write a query to find the average of the department

select dept as dep ,avg(salary) as avg_salary
from employee_df
group by dept

-- COMMAND ----------


--2nd we store the data in a data frame or use it as cte

--3rd we need to join this to the employee table for comparison 

--4th we add a condition where employee salary > avg_salary

with dept_avg as (
  select dept as dep,avg(salary) as avg_salary
from  employee_df
group by dept
)

select * from employee_df e
join dept_avg
on e.dept=dep
where e.salary>avg_salary

-- COMMAND ----------

--This is co related sub query used to solve same problem in a different way
SELECT * FROM employee_df e
WHERE salary > (
    SELECT AVG(salary)
    FROM employee_df
    WHERE dept = e.dept
);

-- COMMAND ----------



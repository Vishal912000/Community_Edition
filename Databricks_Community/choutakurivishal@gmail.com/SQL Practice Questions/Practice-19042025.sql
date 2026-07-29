-- Databricks notebook source
-- MAGIC %run /Users/choutakurivishal@gmail.com/Tables/Departments

-- COMMAND ----------

-- MAGIC %run /Users/choutakurivishal@gmail.com/Tables/Employee_table

-- COMMAND ----------

--03_Find employees who earn more than  average salary of their department

with avg_salary_of_departments as (select dept,avg (salary) as avg_sal from employee_df
group by dept)

select e.* from employee_df e
join avg_salary_of_departments a
on e.dept=a.dept
where e.salary >a.avg_sal

-- COMMAND ----------

--04_Emp from dep where avg salary is more than 80,000

with avg_80000 as (select dept ,avg(salary) as avg_sal
from employee_df
group by dept
having avg_sal>80000)

select * from employee_df e
join avg_80000 as a
on e.dept=a.dept

-- COMMAND ----------

--05_Get empl who have same sal as someone in diff dept

select e1.* from employee_df e1
join employee_df e2
on e1.dept!=e2.dept
and e1.salary=e2.salary

-- COMMAND ----------

--06_List_Employees_who_earn_more_than_highest_paid_employee_in_HR_Department


-- --1st method 
-- select * from employees_df 
-- where salary>(select max(salary) as hr_max from employees_df
-- where dept='HR')


--2nd method 
with hr_cte as (select max(salary) as hr_max from employees_df
where dept='HR'
)

select e.* from employees_df e
join hr_cte h on
e.salary>h.hr_max









-- COMMAND ----------

-- 01_Find the duplicates in the data

select emp_id, count(*) as duplicate from employees_df
group by emp_id
having count(*)>1

-- COMMAND ----------

-- 03_Find employees who are not  in department table

select e.* from employees_df emp
left join departments_df dep
on emp.dept=dep.dept
where dep.dept is null

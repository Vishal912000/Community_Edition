-- Databricks notebook source
-- MAGIC %run /Users/choutakurivishal@gmail.com/Tables/Departments

-- COMMAND ----------


1. **Start with the `employees_df` table** — it has employee data.
2. **Use `DENSE_RANK()`** to give a rank to each employee **within their department**, based on salary (highest salary gets rank 1).
3. **`PARTITION BY dept`** means ranking is done **separately for each department**.
4. **`ORDER BY salary DESC`** means higher salaries get a better (lower) rank.
5. **Put all this in a subquery** — now each row also has a rank column (`d_rank`).
6. **Select only those rows where `d_rank = 1`** — these are the **top-paid employees in each department**.

-- COMMAND ----------

--1st method 
select * from (select * ,dense_rank() over (partition by dept order by salary desc) as d_rank 
from employees_df)
where d_rank=1

-- need to change d_rank=2 for second highest salary

-- COMMAND ----------

--2nd method 
with d_rank as (select * ,dense_rank() over (partition by dept order by salary desc ) as d_rank 
from employees_df )

select e.* from employees_df  e
join d_rank d on e.emp_id=d.emp_id
where d_rank=1

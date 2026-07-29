-- Databricks notebook source
-- MAGIC %run /Users/choutakurivishal@gmail.com/Tables/Departments

-- COMMAND ----------

1. Use the same table twice with aliases `e1` and `e2`.  
2. Do an `INNER JOIN` on `e1.manager_id = e2.emp_id`.  
3. Use `WHERE` to filter rows where `e1.salary > e2.salary`.  
4. Use `SELECT *` to get all columns from the matched rows.

-- COMMAND ----------

select * from employees_df e1
join employees_df e2  on
e1.manager_id=e2.emp_id
where e1.salary>e2.salary


-- COMMAND ----------


### INNER JOIN Condition:

JOIN employees_df e2 ON e1.manager_id = e2.emp_id
### What it means:

1. **We have two versions of the same table:**
   - `e1` is for employees.
   - `e2` is for managers.

2. **Why are we joining?**
   - We want to match each employee (`e1`) with their **actual manager** (`e2`).
   - But in the table, we don’t have the manager’s full details in `e1`, only their `manager_id`.

3. **What does the condition do?**
   - `e1.manager_id = e2.emp_id` says:
     - "Find the row in `e2` where the manager’s `emp_id` matches the `manager_id` from `e1`."
   - This way, we link an employee with their manager’s full data.

4. **Why INNER JOIN?**
   - `INNER JOIN` only keeps the rows where a match is found.
   - So we only get employees who actually have a manager listed in the table.

---

### Simple Summary:
We use `INNER JOIN` to connect each employee with their manager’s details using `manager_id = emp_id`. This lets us compare their salaries or use any other data from both the employee and their manager.

-- Databricks notebook source
-- MAGIC %md
-- MAGIC **A correlated subquery** is a subquery that refers to a column from the outer query. This means the subquery is executed once for each row selected by the outer query.
-- MAGIC
-- MAGIC
-- MAGIC It Cannot run on its own, depends on the outer query.
-- MAGIC
-- MAGIC A correlated subquery uses values from the outer query for its execution and runs row-by-row.

-- COMMAND ----------

--EX:
SELECT *
FROM employees e
WHERE salary > (
    SELECT AVG(salary)
    FROM employees
    WHERE dept = e.dept
);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC  What’s Going On?
-- MAGIC The outer query selects employees from the employees table.
-- MAGIC
-- MAGIC The subquery calculates the average salary in the same department as the current row in the outer query.
-- MAGIC
-- MAGIC The subquery uses e.dept, which comes from the outer query – this makes it correlated.
-- MAGIC
-- MAGIC This process happens for each row.
-- MAGIC
-- MAGIC 🔄 Execution Steps (Row by Row):
-- MAGIC Let’s go through each row:
-- MAGIC
-- MAGIC 🧍 Alice (HR, Salary 5000)
-- MAGIC Subquery runs:
-- MAGIC → SELECT AVG(salary) FROM employees WHERE dept = 'HR'
-- MAGIC → Result: (5000 + 7000 + 5500) / 3 = 5833.33
-- MAGIC
-- MAGIC Is 5000 > 5833.33? ❌ No → Alice excluded
-- MAGIC
-- MAGIC 🧍 Bob (HR, Salary 7000)
-- MAGIC Subquery runs:
-- MAGIC → Same AVG = 5833.33
-- MAGIC
-- MAGIC Is 7000 > 5833.33? ✅ Yes → Bob included
-- MAGIC
-- MAGIC 🧍 Carol (IT, Salary 6000)
-- MAGIC Subquery: AVG in IT = (6000 + 8000)/2 = 7000
-- MAGIC
-- MAGIC 6000 > 7000? ❌ No → Excluded
-- MAGIC
-- MAGIC 🧍 David (IT, Salary 8000)
-- MAGIC 8000 > 7000? ✅ Yes → Included
-- MAGIC
-- MAGIC 🧍 Ethan (HR, Salary 5500)
-- MAGIC 5500 > 5833.33? ❌ No → Excluded
-- MAGIC
-- MAGIC
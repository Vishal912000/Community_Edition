-- Databricks notebook source
SELECT DISTINCT d.dname, COUNT(e.empno) AS emp_count, AVG(e.sal) AS avg_salary
FROM emp e
JOIN dept d ON e.deptno = d.deptno
WHERE e.sal > 2000
GROUP BY d.dname
HAVING COUNT(e.empno) > 2
ORDER BY avg_salary DESC
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 🔍 **Step-by-step Logical Execution**
-- MAGIC
-- MAGIC | Step | Clause | What Happens |
-- MAGIC |------|--------|--------------|
-- MAGIC | 1 | `FROM emp e JOIN dept d` | Join `emp` and `dept` tables |
-- MAGIC | 2 | `ON e.deptno = d.deptno` | Apply the join condition |
-- MAGIC | 3 | `WHERE e.sal > 2000` | Filter rows from the joined result where salary > 2000 |
-- MAGIC | 4 | `GROUP BY d.dname` | Group remaining rows by department name |
-- MAGIC | 5 | `HAVING COUNT(e.empno) > 2` | Keep only those groups with more than 2 employees |
-- MAGIC | 6 | `SELECT DISTINCT d.dname, COUNT(e.empno), AVG(e.sal)` | Select unique department names with employee count and average salary |
-- MAGIC | 7 | `ORDER BY avg_salary DESC` | Sort by average salary in descending order |
-- MAGIC | 8 | `LIMIT 5` | Return only the top 5 rows |
-- MAGIC
-- MAGIC ---
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC
-- MAGIC ### 🔢 **SQL Logical Order of Execution (with Joins)**
-- MAGIC
-- MAGIC | Step | Clause        | Description |
-- MAGIC |------|---------------|-------------|
-- MAGIC | 1    | `FROM`        | Identify source tables, apply **joins**, **subqueries**, **aliases**, and **on conditions** |
-- MAGIC | 2    | `ON`          | Join condition evaluated here during `JOIN` |
-- MAGIC | 3    | `JOIN`        | Tables are joined as per join type (`INNER`, `LEFT`, etc.) |
-- MAGIC | 4    | `WHERE`       | Filters **rows** after joins but **before grouping** |
-- MAGIC | 5    | `GROUP BY`    | Groups rows based on specified columns |
-- MAGIC | 6    | `HAVING`      | Filters **groups** (used with aggregate functions) |
-- MAGIC | 7    | `SELECT`      | Selects columns, expressions, aggregates |
-- MAGIC | 8    | `DISTINCT`    | Removes duplicates (if used) |
-- MAGIC | 9    | `ORDER BY`    | Sorts the final result |
-- MAGIC | 10   | `LIMIT / OFFSET` | Limits rows returned (pagination, etc.) |
-- MAGIC
-- MAGIC ---
-- MAGIC
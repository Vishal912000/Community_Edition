-- Databricks notebook source
-- MAGIC %run /Users/choutakurivishal@gmail.com/Tables/Employee_with_duplicates

-- COMMAND ----------

-- 1. **Create  CTE**, select each `emp_id` from the `employees_df` table.

-- 2. **Use `row_number()`** to assign a unique number to each row **within the same `emp_id`**, ordered by `emp_id`.

-- 3. Now, delete all rows from the CTE **where the row number (`rn`) is greater than 1**, meaning duplicates.

-- COMMAND ----------

with duplicate_cte as (select emp_id,row_number() over (partition by emp_id order by emp_id) as rn
from employees_df)

delete from duplicate_cte where rn>1

-- DELETE FROM employees_df
-- WHERE emp_id IN (
--     SELECT emp_id
--     FROM (
--         SELECT emp_id,
--                ROW_NUMBER() OVER (PARTITION BY emp_id ORDER BY emp_id) AS rn
--         FROM employees_df
--     ) AS sub
--     WHERE rn > 1
-- );


-- COMMAND ----------

```sql
WITH duplicate_cte AS (
    SELECT emp_id,
           ROW_NUMBER() OVER (PARTITION BY emp_id ORDER BY emp_id) AS rn
    FROM employees_df
)

DELETE FROM duplicate_cte WHERE rn > 1;
```
#### 🔹 Step 1: Understand the goal
The purpose of this query is to **find and delete duplicate employee records** based on the `emp_id` field — keeping only one entry for each `emp_id` and removing the rest.

#### 🔹 Step 2: `WITH duplicate_cte AS (...)`
- This part creates a **CTE (Common Table Expression)** — a temporary result set called `duplicate_cte` that we can use like a table.
- Think of it as a way to "prepare" or "filter" the data before doing something with it (in this case, deleting duplicates).

#### 🔹 Step 3: Inside the CTE — `SELECT emp_id, ROW_NUMBER() OVER (...)`
SELECT emp_id,
       ROW_NUMBER() OVER (PARTITION BY emp_id ORDER BY emp_id) AS rn
```

- We're selecting the `emp_id` column from the `employees_df` table.
- Then we're using the **`ROW_NUMBER()` window function** to assign a **number to each row**, starting from 1.

#### 🔹 Step 4: `PARTITION BY emp_id`

- This tells SQL to group rows **with the same `emp_id` together**.
- Each group (with the same `emp_id`) is processed separately.

#### 🔹 Step 5: `ORDER BY emp_id`

- Within each group of the same `emp_id`, rows are **ordered by emp_id**.
- In this specific case, since we’re ordering by the same column we're partitioning on, the order won’t change much — but it’s required by `ROW_NUMBER()`.

#### 🔹 Step 6: `DELETE FROM duplicate_cte WHERE rn > 1`

- This deletes all rows **where the row number is greater than 1**.
- That means:
  - **Keep the first occurrence** of each `emp_id` (row number = 1).
  - **Delete the rest** (row number > 1).

### ✅ Final Result:
After this query runs, your `employees_df` table will have **only one row per `emp_id`**, and all duplicate entries will be gone.

### ❗Note:
In standard SQL, you **can’t delete directly from a CTE** in many databases. Some platforms like **PostgreSQL** allow this with writable CTEs, but in others like **MySQL or SQL Server**, you would need to use a subquery or a different method.

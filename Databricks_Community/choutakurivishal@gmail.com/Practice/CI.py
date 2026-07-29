# Databricks notebook source
# MAGIC %sql
# MAGIC select * from table1 inner join table2
# MAGIC where col in ('abc','pqr')
# MAGIC groupby col2
# MAGIC orderby col3
# MAGIC
# MAGIC
# MAGIC Employee Table
# MAGIC id 	name 	salary 	managerId
# MAGIC 1	Chetan	70000	3
# MAGIC 2	Ram	80000	4
# MAGIC 3	Sarath	60000	Null     
# MAGIC 4	Manish	90000	Null     
# MAGIC
# MAGIC select e.id,e.name,e.salary,e2.salary from employee_table as e  join employee_table e2 on e.id=e2.managerId 
# MAGIC where e.salary>e2.salary
# MAGIC
# MAGIC ogs table
# MAGIC id	num    prev 	next
# MAGIC 1	1	null	1
# MAGIC 2	1	1	1
# MAGIC 3	1	1	2
# MAGIC 4	2	1	1
# MAGIC 5	1	2	2
# MAGIC 6	2	1	2
# MAGIC 7	2	2	null
# MAGIC null	null	2	null
# MAGIC
# MAGIC select id,num ,lag(num) partition by id as prev, lead(num) partition by id as next from ogs_table where num= prev and num=next
# MAGIC
# MAGIC
# MAGIC customer	start_loc	end_loc
# MAGIC c1		New York	Lima
# MAGIC c1		London		New York
# MAGIC c1		Lima		Sao Paulo
# MAGIC c1		Sao Paulo	New Delhi
# MAGIC
# MAGIC c2		Mumbai		Hyderabad
# MAGIC c2		Surat		Pune
# MAGIC c2		Hyderabad	Surat
# MAGIC
# MAGIC c3		Kochi		Kurnool
# MAGIC c3		Lucknow		Agra
# MAGIC c3		Agra		Jaipur
# MAGIC c3		Jaipur		Kochi
# MAGIC
# MAGIC London-NewYork-Lima-Saoulo-Newdelhi
# MAGIC
# MAGIC
# MAGIC select start_loc,end_loc from 
# MAGIC (select customer, start_loc, end_loc from customers where start_loc != end_loc
# MAGIC group by customer)
# MAGIC
# MAGIC
# MAGIC ####Write a python code for Counting Consonants in a Given Word
# MAGIC word = "programming"
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

df_student_Scores (Stud_ID,Subject_ID,Marks)
df_student (stud_ID,Stud_Name )
 
1)Write code so that lists the students who have score more than 50
in all the subjects

# Register the DataFrames as SQL temporary views
df_student_Scores.createOrReplaceTempView("student_scores")
df_student.createOrReplaceTempView("student")

# Spark SQL query to find students who scored more than 50 in all subjects
query = """
SELECT s.Stud_Name
FROM student s
JOIN (
    SELECT Stud_ID
    FROM student_scores
    GROUP BY Stud_ID
    HAVING MIN(Marks) > 50
) ss ON s.Stud_ID = ss.Stud_ID
"""

# Execute the query
df_result = spark.sql(query)

# Show the result
df_result.show()


# COMMAND ----------

how to load multiple files(csv,json,txt) in pyspark from a folder ,I need to load only csv files


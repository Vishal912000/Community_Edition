# Databricks notebook source
from pyspark.sql import Row

# Sample 20 employees
employee_data = [
    Row(id=1, name='Alice', salary=70000, dept='HR', dept_id=101, age=30, sex='F', experience_years=5, city='New York'),
    Row(id=2, name='Bob', salary=80000, dept='Finance', dept_id=102, age=35, sex='M', experience_years=8, city='Chicago'),
    Row(id=3, name='Charlie', salary=75000, dept='IT', dept_id=103, age=29, sex='M', experience_years=4, city='San Francisco'),
    Row(id=4, name='David', salary=72000, dept='IT', dept_id=103, age=28, sex='M', experience_years=3, city='Seattle'),
    Row(id=5, name='Eva', salary=68000, dept='HR', dept_id=101, age=26, sex='F', experience_years=2, city='New York'),
    Row(id=6, name='Frank', salary=90000, dept='Finance', dept_id=102, age=40, sex='M', experience_years=10, city='Boston'),
    Row(id=7, name='Grace', salary=82000, dept='Marketing', dept_id=104, age=33, sex='F', experience_years=7, city='Denver'),
    Row(id=8, name='Helen', salary=77000, dept='Marketing', dept_id=104, age=32, sex='F', experience_years=6, city='Miami'),
    Row(id=9, name='Ivan', salary=88000, dept='Sales', dept_id=105, age=38, sex='M', experience_years=9, city='Dallas'),
    Row(id=10, name='Judy', salary=69000, dept='Sales', dept_id=105, age=27, sex='F', experience_years=3, city='Atlanta'),
    Row(id=11, name='Kevin', salary=73000, dept='IT', dept_id=103, age=31, sex='M', experience_years=6, city='Austin'),
    Row(id=12, name='Luna', salary=76000, dept='HR', dept_id=101, age=29, sex='F', experience_years=4, city='Chicago'),
    Row(id=13, name='Mike', salary=85000, dept='Finance', dept_id=102, age=36, sex='M', experience_years=8, city='New York'),
    Row(id=14, name='Nina', salary=67000, dept='Sales', dept_id=105, age=25, sex='F', experience_years=2, city='Houston'),
    Row(id=15, name='Oscar', salary=92000, dept='IT', dept_id=103, age=39, sex='M', experience_years=11, city='San Jose'),
    Row(id=16, name='Pam', salary=64000, dept='Marketing', dept_id=104, age=24, sex='F', experience_years=1, city='Los Angeles'),
    Row(id=17, name='Quinn', salary=81000, dept='Finance', dept_id=102, age=34, sex='F', experience_years=7, city='Phoenix'),
    Row(id=18, name='Ron', salary=70000, dept='Sales', dept_id=105, age=30, sex='M', experience_years=5, city='San Diego'),
    Row(id=19, name='Sara', salary=69000, dept='HR', dept_id=101, age=28, sex='F', experience_years=3, city='Chicago'),
    Row(id=20, name='Tom', salary=93000, dept='IT', dept_id=103, age=40, sex='M', experience_years=12, city='Seattle')
]

# Create DataFrame
employee_df = spark.createDataFrame(employee_data)

# Register as a temporary view
employee_df.createOrReplaceTempView("employee_df")

# Show the data (optional)
print(f"table name is employee_df")
# display(employee_df)

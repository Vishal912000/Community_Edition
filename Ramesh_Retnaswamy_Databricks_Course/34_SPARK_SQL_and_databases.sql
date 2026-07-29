-- Databricks notebook source
-- MAGIC %md 
-- MAGIC #Creation of a database :-

-- COMMAND ----------

--To create a database we need to use below command
create database demo

-- COMMAND ----------

--If we already have a database with same name it will throw an error saying databases already exists ,at that time we need to use 
create database if not exists demo

--this code checks if databases exist or not first ,if does not exist  then only  it will create a database

-- COMMAND ----------

show databases
--This command is used to show the list of all the databases 

desc database database_name 
-- or  describe database database_name 
--This command is used to all the information of the database_name 

show tables 
--This command is used to show the list of all tables

select current_database()
--This command is used to select the current_database

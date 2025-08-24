# Databricks notebook source
# MAGIC %md #Please check wafa studies videos also
# MAGIC #Databricks widgets:-
# MAGIC
# MAGIC 1)Widgets are interactive elements which can be added to notebook to improve their functionality 
# MAGIC
# MAGIC 2)Widgets are small tool like buttons that help you interact within notebook easily
# MAGIC
# MAGIC 3)widgets are used when we are exploring results of a single query with different parameters (like we can retrieve the data for different environments and dates by chnaging the date in widgets
# MAGIC
# MAGIC 4)widgets can make notebooks more re-usable
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC Types of widgets:-
# MAGIC
# MAGIC 1)text
# MAGIC
# MAGIC 2)dropdown
# MAGIC
# MAGIC 3)multiselect
# MAGIC
# MAGIC 4)comboBox

# COMMAND ----------

dbutils.widgets.help()
#This command is used to check any help regarding widgets 

# COMMAND ----------

dbutils.widgets.help('get')

# COMMAND ----------

#Text Widget 

#Here we are creating a text widget by giving parameters name,defaultValue,label

#syntax:-dbutils.widgets.text(name='String_name',defaultValue='default_name',label='label_name')

dbutils.widgets.text(name='date_text',defaultValue='01-Jan-2024',label='date_tt')


# COMMAND ----------

# MAGIC %md
# MAGIC #parameters are same for dropdown ,combobox,multiselect

# COMMAND ----------

#dropdown Widget: dropdown is like a list where we can select from a list but cannot change the list

#Here we are creating a dropdown widget by giving parameters name,defaultValue,choices(sequence),label

dbutils.widgets.dropdown(name='date_dropdown',defaultValue='01-Jan-2024',choices=['01-Jan-2024','01-Feb-2024','01-Mar-2024','01-Apr-2024'],
                         label='date_dd')


# COMMAND ----------

#combobox:combobox is like a list where we can select from the given list  and also add a new item to the list

#Here also we are creating a dropdown widget by giving parameters name,defaultValue,choices(sequence),label



dbutils.widgets.combobox(name='date_combobox',defaultValue='01-Jan-2024',choices=['01-Jan-2024','01-Feb-2024','01-Mar-2024','01-Apr-2024'],
                         label='date_cb')

# COMMAND ----------

#multiselect:multiselect is also like a list but here we can select multiple items from the given list only

#Here also we are creating a dropdown widget by giving parameters name,defaultValue,choices(sequence),label
dbutils.widgets.multiselect(name='date_multiselect',defaultValue='01-Jan-2024',choices=['01-Jan-2024','01-Feb-2024','01-Mar-2024','01-Apr-2024'],
                         label='date_ms')

# COMMAND ----------

# get command:-  we need to use the ""get"" command to read the value inside the widget

#syntax:-dbutils.widgets.get('name of the widget given' )

display(dbutils.widgets.get('date_text'))

#we can also store this coomand in a variable and display that variable

date=dbutils.widgets.get('date_text')

display(date)

# COMMAND ----------

#remove:-remove command is used to remove a widget which is inside the parameters

#we should call a widget by its name 

dbutils.widgets.remove('date_text')

# COMMAND ----------

# MAGIC %md
# MAGIC #removeAll:-remove all is used to remove all  widgets in notebook
# MAGIC
# MAGIC syntax:-dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.removeAll()
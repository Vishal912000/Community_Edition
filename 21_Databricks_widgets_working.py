# Databricks notebook source
#In Air india project we used widgets for  az_dev,az_prd and for dates 

# COMMAND ----------

#text widget
dbutils.widgets.text(name='date_text',defaultValue='',label='text_value')
text_widget=dbutils.widgets.get('date_text')
display(text_widget)

# COMMAND ----------

#dropdown widget
dbutils.widgets.dropdown(name='date_dropdown',defaultValue='01-Jan-2024',choices=['01-Jan-2024','01-Feb-2024','01-Mar-2024'],label='dropdown_value')
dropdown_widget=dbutils.widgets.get('date_dropdown')
display(dropdown_widget)


# COMMAND ----------

#combobox widget
dbutils.widgets.combobox(name='date_combobox',defaultValue='',choices=['01-Jan-2024','01-Feb-2024','01-Mar-2024'],label='combobox_value')
combobox_widget=dbutils.widgets.get('date_combobox')
display(combobox_widget)


# COMMAND ----------

#multiselect widget
dbutils.widgets.multiselect(name='date_multiselect',defaultValue='01-Jan-2024',choices=['01-Jan-2024','01-Feb-2024','01-Mar-2024'],label='multiselect')
multiselect_widget=dbutils.widgets.get('date_multiselect')
display(multiselect_widget)

# COMMAND ----------


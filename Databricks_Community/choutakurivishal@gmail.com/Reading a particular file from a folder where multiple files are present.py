# Databricks notebook source
# Define the folder path
folder_path = "/mnt/your-folder-path/"

# List all files in the folder
files = dbutils.fs.ls(folder_path)

# Filter only CSV files
csv_files = [file.path for file in files if file.path.endswith(".csv")]

# Create a dictionary to store DataFrames
dataframes = {}

# Load each CSV file into a separate DataFrame
for file_path in csv_files:
    file_name = file_path.split("/")[-1].replace(".csv", "")  # Extract file name without extension
    dataframes[file_name] = spark.read.format("csv").option("header", "true").load(file_path)

# Accessing a specific DataFrame
for key, df in dataframes.items():
    print(f"Displaying data for {key}:")
    df.show()
Explanation:
Filter CSV Files:

Use dbutils.fs.ls to list all files in the directory and filter those ending with .csv.
Dynamic DataFrame Storage:

Use a dictionary (dataframes) where the keys are the file names (without the .csv extension) and the values are the corresponding DataFrames.
Loop Through Files:

Dynamically read each file and store the resulting DataFrame in the dictionary.
Accessing DataFrames:

Access any DataFrame by its key (file name). For example, dataframes['file1'] will give you the DataFrame for file1.csv.


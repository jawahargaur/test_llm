# Databricks notebook source


# COMMAND ----------

# Set the Delta Lake catalog path
catalog_path = "/path/to/catalog"

# Define the table name
table_name = "main.default.bronze_pdf_landing_raw"

# Create the DataFrame
df = spark.read.table(f"{catalog_path}.{table_name}")

# Display the DataFrame
df.show()

# COMMAND ----------

df = spark.read.table("main.default.bronze_pdf_landing_raw")


# COMMAND ----------

display(df)

# COMMAND ----------

df = spark.read.table("main.default.gold_pdf_landing_chunked")

# COMMAND ----------

display(df)

# COMMAND ----------

pandas_df = df.toPandas()

# COMMAND ----------

display(pandas_df)

# COMMAND ----------

df = spark.read.table("main.default.silver_pdf_landing_parsed")
path = "/Workspace/Repos/odl_user_1291024@databrickslabs.com/test_llm/data/silver.csv"
pandas_df = df.toPandas()
pandas_df.to_csv(path, index=False, escapechar=',')

# COMMAND ----------

path = "/Workspace/Repos/odl_user_1291024@databrickslabs.com/test_llm/data/bronze.csv"


# Write the DataFrame as a Delta table to the specified path
df.write.format("csv").mode("overwrite").save(path)

-- Databricks notebook source
-- MAGIC %md ### IMPORT PACKAGES AND CREATE DATAFRAMES

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Import pyspark packages
-- MAGIC from pyspark.sql.types import *
-- MAGIC from pyspark.sql.functions import *
-- MAGIC
-- MAGIC #Creating clinicaltrial_2023 Dataframe
-- MAGIC Schema = StructType([
-- MAGIC            StructField("Id", StringType()),
-- MAGIC            StructField("Study_Title", StringType()),
-- MAGIC            StructField("Acronym", StringType()),
-- MAGIC            StructField("Status", StringType()),
-- MAGIC            StructField("Conditions", StringType()),
-- MAGIC            StructField("Interventions", StringType()),
-- MAGIC            StructField("Sponsor", StringType()),
-- MAGIC            StructField("Collaborators", StringType()),
-- MAGIC            StructField("Enrollment", StringType()),
-- MAGIC            StructField("Funder_Type", StringType()),
-- MAGIC            StructField("Type", StringType()),
-- MAGIC            StructField("Study_Design", StringType()),
-- MAGIC            StructField("Start", StringType()),
-- MAGIC            StructField("Completion", StringType())
-- MAGIC            ])
-- MAGIC
-- MAGIC clinicaltrial_2023 = sc.textFile("/FileStore/tables/clinicaltrial_2023.csv")
-- MAGIC
-- MAGIC clinicaltrial_2023 = clinicaltrial_2023.map(lambda line: line.replace('"','').replace(',,','').split("\t"))
-- MAGIC
-- MAGIC header = clinicaltrial_2023.first()
-- MAGIC
-- MAGIC clinicaltrial_2023 = clinicaltrial_2023.map(lambda row: row + ["NULL" for i in range(len(header) - len(row))] if len(row) < len(header) else row)
-- MAGIC
-- MAGIC clinicaltrial_2023 = clinicaltrial_2023.map(lambda row: [col if col != "" else "NULL" for col in row])
-- MAGIC
-- MAGIC clinicaltrial_2023 = clinicaltrial_2023.filter(lambda x: x != header)
-- MAGIC
-- MAGIC clinicaltrial_2023DF = spark.createDataFrame(clinicaltrial_2023, Schema)
-- MAGIC
-- MAGIC clinicaltrial_2023DF.display(10, truncate= False)
-- MAGIC
-- MAGIC #Creating Pharma Dataframe
-- MAGIC pharmaDF = spark.read.csv("/FileStore/tables/pharma.csv", header = "True")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Creating clinicaltrial_2023  and Pharma View 
-- MAGIC clinicaltrial_2023DF.createOrReplaceTempView('clinicaltrial_2023')
-- MAGIC pharmaDF.createOrReplaceTempView('Pharma')

-- COMMAND ----------

-- MAGIC %md ### DISTINCT COUNT OF STUDIES

-- COMMAND ----------

--1. Distinct count of studies
SELECT DISTINCT(Count('Id')) as Studies_Count
FROM clinicaltrial_2023

-- COMMAND ----------

-- MAGIC %md ### TYPE OF STUDIES AND THEIR FREQUENCIES

-- COMMAND ----------

SELECT Type, count(*) as count
FROM clinicaltrial_2023
WHERE Type != 'NULL'
GROUP BY Type
ORDER BY count DESC

-- COMMAND ----------

-- Clean multiple values in new view 
CREATE OR REPLACE TEMP VIEW clinicaltrial_2023_ as SELECT explode(split(Conditions, r'\|')) Condition
FROM clinicaltrial_2023
WHERE Conditions != ''

-- COMMAND ----------

-- MAGIC %md ### TOP 5 CONDITIONS AND THEIR FREQUENCIES

-- COMMAND ----------

--3. Top 5 conditions and their frequencies
SELECT Condition, count(*) as Count
FROM clinicaltrial_2023_
GROUP BY Condition
ORDER BY Count DESC
LIMIT 5

-- COMMAND ----------

-- MAGIC %md ###TOP 10 MOST COMMON SPONSORS THAT ARE NOT PHARMACEUTICAL COMPANIES

-- COMMAND ----------

SELECT Sponsor, count(*) as Count
FROM clinicaltrial_2023
WHERE Sponsor NOT IN(SELECT Parent_Company from Pharma)
GROUP BY Sponsor
ORDER BY Count DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md ###NUMBER OF STUDIES PER MONTH IN 2023

-- COMMAND ----------

SELECT right(left(Completion,7 ), 2) as Month, count(*) as Year_2023
FROM clinicaltrial_2023
WHERE Completion LIKE '2023%'
AND Status = 'COMPLETED'
GROUP BY Month
ORDER BY Month

-- COMMAND ----------

-- MAGIC %md ###TOP 10 COMPLETED STUDIES WITH THE HIGHEST DURATIONS

-- COMMAND ----------

WITH StudyDates AS (
                    SELECT Id,
                    Status,
                    Study_Title,
                    CAST(Start AS DATE) AS StartDate,
                    CAST(left(Completion,7 ) AS DATE) AS CompletionDate,
                    DATEDIFF(DAY, CAST(Start AS DATE), CAST(LEFT(Completion, 7) AS DATE)) AS DurationInDays
                    FROM clinicaltrial_2023 
)

SELECT Study_Title, DurationInDays
FROM StudyDates
WHERE Status = 'COMPLETED'
ORDER BY DurationInDays DESC

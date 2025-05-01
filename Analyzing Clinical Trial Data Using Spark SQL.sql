-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##  **_Analyzing Clinical Trial Data Using Spark SQL_**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### **_Introduction_**
-- MAGIC This project explores a clinical trial dataset using distributed data processing tool(Apache Spark - Spark Sql) to uncover patterns, conditions of interests and length of studies. The goal is to derive useful insights on into prevalent medical conditions, study types, and trial durations with diabetes related studies as a focal point.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### **_Objective_**
-- MAGIC - Determine the unique study types.
-- MAGIC - Rank the most common medical conditions in the clinical trial (Top 10).
-- MAGIC - Determine the mean duration of concluded clinical trials.
-- MAGIC - Explore and visualize the trend in completed diabetes related trials over time.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### _**Dataset Overview**_
-- MAGIC Source: The dataset ```Clinicaltrial_16012025.csv``` was sourced from [clinicaltrials.gov](https://clinicaltrials.gov/). <br>
-- MAGIC File Format: .csv with headers on the first row. <br>
-- MAGIC Attributes: The dataset comprises of 14 columns.<br>
-- MAGIC Records: The dataset comprises of 522660 rows.<br>
-- MAGIC Size: 200MB

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicalTrials = spark.read.csv(
-- MAGIC     'dbfs:/FileStore/tables/Clinicaltrial_16012025.csv',
-- MAGIC     header=True,
-- MAGIC     inferSchema=True,
-- MAGIC     multiLine=True,
-- MAGIC     quote='"',
-- MAGIC     escape='"'
-- MAGIC )
-- MAGIC
-- MAGIC clinicalTrials.printSchema()
-- MAGIC
-- MAGIC clinicalTrials.show(5, truncate=False)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### _**Data Cleaning and Preprocessing**_
-- MAGIC The dataset is cleaned removing spaces in the column headers for ease of use in code. This ensures that the column names are Sql friendly and follow common Sql and Python naming conventions. E.g <br>
-- MAGIC NCT Number → NCTNumber <br>
-- MAGIC Study Type → StudyType<br>
-- MAGIC <br>
-- MAGIC The StartDate and CompletionDate date data types are coverted from string to DateType using ``` to_date``` and ```col```  functions, with the format MMM-dd-yyyy. E.g 'April 23, 2025'.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import to_date, col
-- MAGIC
-- MAGIC # Remove spaces from column names
-- MAGIC clinicalTrials = clinicalTrials.toDF(*[c.replace(" ", "").replace(":", "") for c in clinicalTrials.columns])
-- MAGIC
-- MAGIC # Convert date columns to proper date format
-- MAGIC clinicalTrials = clinicalTrials.withColumn("StartDate", to_date(col("StartDate"), "MMMM dd, yyyy")) \
-- MAGIC                                .withColumn("CompletionDate", to_date(col("CompletionDate"), "MMMM dd, yyyy"))
-- MAGIC
-- MAGIC clinicalTrials.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicalTrials.createOrReplaceTempView("clinical_trials")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### _**Exploratory Data Analysis**_

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC **Q1. Study Type Frequency** <br>
-- MAGIC Identified and quantified the distinct study types, ranking them from most frequent to least frequent.

-- COMMAND ----------

SELECT DISTINCT StudyType, COUNT(*) AS StudyType_Frequency
FROM clinical_trials
WHERE StudyType IS NOT NULL
GROUP BY StudyType
ORDER BY StudyType_Frequency DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Q2. Top 10 Conditions Studied** <br>
-- MAGIC Determined the top 10 most studied conditions in the clinical trial by parsing and aggregating the Conditions column. The records with multiple conditions are handled using ``` EXPLODE```  and ``` SPLIT``` .

-- COMMAND ----------

SELECT Condition AS Conditions, COUNT(*) AS Condition_Count
FROM (
    SELECT EXPLODE(SPLIT(Conditions, '\\|')) AS Condition
    FROM clinical_trials
    WHERE Conditions IS NOT NULL
) t
GROUP BY Condition
ORDER BY Condition_Count DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Q3. Average Trial Duration.** <br>
-- MAGIC Calculated the mean period/duration of completed trials in months i.e the trials with completion date not null or empty.

-- COMMAND ----------

SELECT ROUND(AVG(month_interval), 0) AS Avg_Trial_Duration_Months
FROM (
    SELECT StartDate, CompletionDate, 
           MONTHS_BETWEEN(CompletionDate, StartDate) AS month_interval
    FROM clinical_trials
    WHERE CompletionDate IS NOT NULL AND StartDate IS NOT NULL
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Q4. Diabetes-Related Trial Trends.** <br>
-- MAGIC Evaluated and visualized the progression/trend of completed diabetes related trials over the years.

-- COMMAND ----------

SELECT YEAR(clt.CompletionDate) AS Trial_Completion_Year, 
       COUNT(*) AS Diabetes_Related_Trial
FROM clinical_trials clt
WHERE clt.StudyStatus = 'COMPLETED'
  AND LOWER(clt.Conditions) LIKE '%diabetes%'
  AND clt.CompletionDate IS NOT NULL
GROUP BY Trial_Completion_Year
ORDER BY Trial_Completion_Year

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### **_Key Findings_**<br>
-- MAGIC Most Frequent Study Type: Interventional. <br>
-- MAGIC Top Conditions Studied: Healthy. <br>
-- MAGIC Average Trial Duration: Approx. 36 months. <br>
-- MAGIC Completed Diabetes Trials Trend: The graph shows an a steady increase from the year 2000, peaking between 2015 and 2020, and then a sharp decline towards 2025.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### **_Conclusion_**
-- MAGIC This analysis reveals some key findings from the clinical trial dataset using PySpark and Sql.<br> It allows healthcare stakeholders and researchers to gain knowledge into frequently studied conditions and assess trial timelines.
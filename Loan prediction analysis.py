# Databricks notebook source
df=spark.table("default.loan")
df.show()


# COMMAND ----------

from pyspark.sql.functions import col
# filling null in loan amount term with mode
mode_term_df = df.groupBy("Loan_Amount_Term").count().orderBy(col("count").desc())
mode_term = mode_term_df.first()["Loan_Amount_Term"]
df = df.fillna({"Loan_Amount_Term": mode_term})

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Banks consider total family income for loan approval, not individual income alone.
# MAGIC
# MAGIC

# COMMAND ----------

# Add ApplicantIncome and CoapplicantIncome into a new column: Total_Income.

from pyspark.sql.functions import col

df = df.withColumn("Total_Income", col("ApplicantIncome") + col("CoapplicantIncome"))
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Approximate monthly EMI by dividing LoanAmount by Loan_Amount_Term.

# COMMAND ----------

df_emi=df.withColumn("emi",col("LoanAmount")/col("Loan_amount_term"))
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate Income_to_Loan Ratio
# MAGIC Indicates how many times their income covers the loan, useful for assessing credit risk.

# COMMAND ----------

df_in=df_emi.withColumn("Income_to_loan",col("total_income")/col("loanAmount"))
df.show()

# COMMAND ----------

df_in.show()

# COMMAND ----------

# MAGIC %md
# MAGIC  Average Loan Amount per Property_Area

# COMMAND ----------

df_in.printSchema()

# COMMAND ----------

from pyspark.sql.functions import aggregate,avg

df_avg_loan=df_in.groupBy("property_area").avg("LoanAmount")
df_avg_loan.show()

# COMMAND ----------

# MAGIC %md
# MAGIC  Approval Rate by Education Level

# COMMAND ----------

df_approve=df_in.groupBy("Loan_Status","education").count()
df_approve.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Gender-wise Loan Approval Rate

# COMMAND ----------

df.groupBy("Gender", "Loan_Status").count().show()


# COMMAND ----------

df_hist=df_in.groupBy("Credit_History","Loan_Status").count()
df_hist.show()

# COMMAND ----------

df_in.show()

# COMMAND ----------

# MAGIC %md
# MAGIC High EMI to Income Ratio Flag (Risk Indicator)

# COMMAND ----------

from pyspark.sql.functions import when

df = df.withColumn("High_EMI_Burden",
                   when((col("EMI") / col("Total_Income")) > 0.5, 1).otherwise(0))


# COMMAND ----------

# MAGIC %md
# MAGIC Scenario Task 2. Eligible Applicants for Pre-Approved Offer
# MAGIC 📝 Scenario: The bank wants to find graduates (Education = Graduate) with:
# MAGIC
# MAGIC Credit_History = 1
# MAGIC
# MAGIC Total_Income >= 5000
# MAGIC
# MAGIC to send pre-approved loan offers.
# MAGIC
# MAGIC Objective: Create a DataFrame with:
# MAGIC
# MAGIC Loan_ID
# MAGIC
# MAGIC Education
# MAGIC
# MAGIC Total_Income
# MAGIC
# MAGIC Credit_History
# MAGIC
# MAGIC Loan_Status

# COMMAND ----------

df_in.show()

# COMMAND ----------

from pyspark.sql.functions import filter


df_1 = df_in.select("Loan_ID", "Credit_History", "Education", "Total_Income", "Loan_Status") \
    .filter(col("Credit_History") >= 1) \
    .filter(col("Total_Income") >= 234) \
    .where("Education = 'Graduate'")

df_1.show()


# COMMAND ----------

from pyspark.sql.functions import count

df_approved = df_in.filter(col("Loan_Status") == "Y") \
                .groupBy("Property_Area") \
                .agg(count("*").alias("Approved_Loans"))
df_approved.show()


# COMMAND ----------

from pyspark.sql.functions import count

df_area = df_in.groupBy("Property_Area").agg(count("*").alias("Total_Applicants"))
df_area.show()


# COMMAND ----------

# MAGIC %md
# MAGIC . Combine Total Applicants and Approved Loans

# COMMAND ----------


# df_combine=df_approved.join(df_area,on="Property_Area",how="inner")
df_summary = df_area.join(df_approved, on="Property_Area", how="left")
df_summary.show()

# df_combine.show()

# COMMAND ----------

df_in.show()

# COMMAND ----------

from pyspark.sql.functions import when,col
df_risk = df_in.withColumn("Risk_Category",
                        when((col("Credit_History") == 1) & (col("Income_to_Loan") >= 3), "Low Risk")
                        .when((col("Credit_History") == 1) & (col("Income_to_Loan") < 3), "Medium Risk")
                        .otherwise("High Risk")) \
            .select("Loan_ID", "Credit_History", "Income_to_Loan", "Risk_Category")
df_risk.show()

# COMMAND ----------

# MAGIC %md
# MAGIC List the distinct Education levels present.

# COMMAND ----------

df_in.createOrReplaceTempView("loan")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from loan limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select DISTINCT Education from  loan;

# COMMAND ----------

# MAGIC %md
# MAGIC Find the total number of approved loans.

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as Approved_loans from loan where upper(loan_status)='Y';

# COMMAND ----------

# MAGIC %md
# MAGIC Find the average LoanAmount for applicants who are Self_Employed.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select avg(loanAmount) as avg_amount from loan where lower(Self_Employed)="yes";
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Find average ApplicantIncome for each Education level.

# COMMAND ----------

# MAGIC %sql
# MAGIC select avg(ApplicantIncome) as avg_applicant_income , education from loan group by education;

# COMMAND ----------

# MAGIC %md
# MAGIC Find total number of male and female applicants.

# COMMAND ----------

# MAGIC %sql
# MAGIC select gender ,count(*) as total from loan where gender is not null group by Gender;
# MAGIC
# MAGIC -- not showing count of null gender

# COMMAND ----------

# MAGIC %md
# MAGIC Get the average LoanAmount for approved and non-approved loans separately.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select loan_status ,ROUND(avg(loanAmount),2) AS avg_Amount from loan group by loan_status;

# COMMAND ----------

# MAGIC %md
# MAGIC Find the Loan_IDs where ApplicantIncome is greater than CoapplicantIncome.

# COMMAND ----------

# MAGIC %sql
# MAGIC select loan_id from loan where ApplicantIncome > CoapplicantIncome;

# COMMAND ----------

# MAGIC %md
# MAGIC Get Education and average LoanAmount for applicants with Credit_History = 1.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select education,avg(LoanAmount) from loan where Credit_History =1 group by education;

# COMMAND ----------

# MAGIC %md
# MAGIC Write a query to find pairs of loans from the same Property_Area.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from loan;

# COMMAND ----------

# MAGIC %md
# MAGIC Display Loan_ID, Property_Area, LoanAmount, and the average LoanAmount in that Property_Area (using window function).
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select Loan_id,loanAmount, avg(loanamount) over() from loan;

# COMMAND ----------

# MAGIC %md
# MAGIC Display Loan_ID, ApplicantIncome, and rank of ApplicantIncome within each Property_Area (highest income = rank 1).
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select loan_id,applicantincome, property_Area,rank() over(partition by property_area order by applicantincome desc) as rank from loan;

# COMMAND ----------

# MAGIC %md
# MAGIC Display Loan_ID, LoanAmount, and dense rank of LoanAmount across entire table.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select loan_id,loanamount, dense_rank() over(order by loanamount desc) from loan;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC To find the average LoanAmount, then display Loan_ID, LoanAmount, and a column indicating:
# MAGIC ‘Above Average’ if LoanAmount > average
# MAGIC
# MAGIC ‘Below Average’ otherwise
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH avg_loan AS (
# MAGIC   SELECT AVG(LoanAmount) AS avg_loanamount
# MAGIC   FROM loan
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   Loan_ID,
# MAGIC   LoanAmount,
# MAGIC   CASE 
# MAGIC     WHEN LoanAmount > avg_loanamount THEN 'Above Average'
# MAGIC     ELSE 'Below Average'
# MAGIC   END AS Loan_Comparison
# MAGIC FROM loan, avg_loan;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Write a CTE to calculate Total_Income (ApplicantIncome + CoapplicantIncome) for each applicant, then display:
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- with total_income as 
# MAGIC -- (
# MAGIC --       select applicantincome,coapplicantincome, 
# MAGIC --       (applicantincome+coapplicantincome) as total_income
# MAGIC --       from loan
# MAGIC -- )
# MAGIC
# MAGIC WITH total_income_cte AS (
# MAGIC   SELECT
# MAGIC     Loan_ID,
# MAGIC     ApplicantIncome,
# MAGIC     CoapplicantIncome,
# MAGIC     (ApplicantIncome + CoapplicantIncome) AS Total_Income
# MAGIC   FROM loan
# MAGIC ), filter_loan AS (
# MAGIC   select loan_id,total_income FROM loan
# MAGIC   where total_income>6000
# MAGIC )
# MAGIC
# MAGIC select * from filter_loan;

# COMMAND ----------

# MAGIC %sql
# MAGIC select applicantincome, dense_rank() over(order by applicantincome desc) as rnk
# MAGIC from loan limit 5;

# COMMAND ----------

df_in.show()

# COMMAND ----------

df_in.write.mode("error").saveAsTable("filtered_loans")


# COMMAND ----------

df_filtered= spark.table("filtered_loans")

# COMMAND ----------

df_filtered.show(5)
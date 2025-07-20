# Bank Loan Risk Analysis Pipeline
This project analyses bank loan data to understand applicants’ repayment capacity, risk levels, and approval patterns using PySpark and SQL.


## Tools
PySpark – for data cleaning and transformation

SQL – for data analysis and insights

Databricks – as the development and execution platform


## What I Did
Loaded and cleaned the bank loan dataset (handled missing values like Loan_Amount_Term).

Created new features:

Total_Income to calculate family income.

EMI to estimate repayment burden.

Income_to_Loan Ratio to assess repayment capacity.

High EMI Burden Flag to identify risky applicants.

Categorised applicants into Low, Medium, High risk levels.

Filtered eligible customers for pre-approved offers based on income, education, and credit history.

Analysed approval patterns using SQL (e.g., approval rates by education, gender, property area, ranking incomes).


## SQL Queries Executed
Found average LoanAmount per Property_Area.

Calculated approval rates by education.

Analysed gender-wise approval counts.

Used window functions for:

Ranking ApplicantIncome within Property_Area.

Dense ranking LoanAmounts across table.

Comparing individual LoanAmount with average (Above/Below Average classification).

Created CTEs to calculate Total_Income and filter records with income > 6000.

## Use Case in Banking

Banks can know who can repay loans easily.
They can avoid giving loans to risky people, reducing losses.
They can approve loans faster for safe customers.
Helps banks target the right people for offers, growing their business.
Makes their loan decisions smarter and data-driven.


### Learnt how to clean and analyse bank data using PySpark and SQL to help banks make smarter and safer loan decisions.

Using PySpark transformations and SQL for business analysis.

Categorising applicants based on credit risk levels.

Filtering data for targeted offers and decisions.

Applying window functions, CTEs, and joins for advanced SQL analysis.



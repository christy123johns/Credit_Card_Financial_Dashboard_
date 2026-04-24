
-- SQL Query to create and import data from csv files:

-- 0. Create a database 
-- CREATE DATABASE ccdb;
--------------------------------------------------------------

-- 1. Switch to your database (CRITICAL STEP)
USE ccdb;
GO

-- 2. Cleanup old attempts
DROP TABLE IF EXISTS cc_detail;
DROP TABLE IF EXISTS cust_detail;
DROP TABLE IF EXISTS cc_detail_staging;
DROP TABLE IF EXISTS cust_detail_staging;
GO

-- 3. Create Final CC Table
CREATE TABLE cc_detail (
    Client_Num INT, Card_Category VARCHAR(20), Annual_Fees INT, Activation_30_Days INT,
    Customer_Acq_Cost INT, Week_Start_Date DATE, Week_Num VARCHAR(20), Qtr VARCHAR(10),
    current_year INT, Credit_Limit DECIMAL(10,2), Total_Revolving_Bal INT,
    Total_Trans_Amt INT, Total_Trans_Ct INT, Avg_Utilization_Ratio DECIMAL(10,3),
    Use_Chip VARCHAR(10), Exp_Type VARCHAR(50), Interest_Earned DECIMAL(10,3), Delinquent_Acc VARCHAR(5)
);

-- 4. Create Final Customer Table
CREATE TABLE cust_detail (
    Client_Num INT, Customer_Age INT, Gender VARCHAR(5), Dependent_Count INT,
    Education_Level VARCHAR(50), Marital_Status VARCHAR(20), State_cd VARCHAR(50),
    Zipcode VARCHAR(20), Car_Owner VARCHAR(5), House_Owner VARCHAR(5),
    Personal_Loan VARCHAR(5), Contact VARCHAR(50), Customer_Job VARCHAR(50),
    Income INT, Cust_Satisfaction_Score INT
);
GO

-- 5. Load CC Data via Staging
CREATE TABLE cc_detail_staging (
    Client_Num VARCHAR(max), Card_Category VARCHAR(max), Annual_Fees VARCHAR(max),
    Activation_30_Days VARCHAR(max), Customer_Acq_Cost VARCHAR(max),
    Week_Start_Date VARCHAR(max), Week_Num VARCHAR(max), Qtr VARCHAR(max),
    current_year VARCHAR(max), Credit_Limit VARCHAR(max), Total_Revolving_Bal VARCHAR(max),
    Total_Trans_Amt VARCHAR(max), Total_Trans_Ct VARCHAR(max),
    Avg_Utilization_Ratio VARCHAR(max), Use_Chip VARCHAR(max), Exp_Type VARCHAR(max),
    Interest_Earned VARCHAR(max), Delinquent_Acc VARCHAR(max));

BULK INSERT cc_detail_staging FROM 'C:\Users\kchri\Downloads\Credit_Card_Risk\Data\credit_card.csv' WITH (FORMAT = 'CSV', FIRSTROW = 2, CODEPAGE = '65001', TABLOCK);

INSERT INTO cc_detail SELECT TRY_CAST(Client_Num AS INT), Card_Category, TRY_CAST(Annual_Fees AS INT), TRY_CAST(Activation_30_Days AS INT), TRY_CAST(Customer_Acq_Cost AS INT), TRY_CONVERT(DATE, Week_Start_Date, 103), Week_Num, Qtr, TRY_CAST(current_year AS INT), TRY_CAST(Credit_Limit AS DECIMAL(10,2)), TRY_CAST(Total_Revolving_Bal AS INT), TRY_CAST(Total_Trans_Amt AS INT), TRY_CAST(Total_Trans_Ct AS INT), TRY_CAST(Avg_Utilization_Ratio AS DECIMAL(10,3)), Use_Chip, Exp_Type, TRY_CAST(Interest_Earned AS DECIMAL(10,3)), Delinquent_Acc FROM cc_detail_staging;
DROP TABLE cc_detail_staging;

-- 6. Load Customer Data via Staging
CREATE TABLE cust_detail_staging (
    Client_Num VARCHAR(max), Customer_Age VARCHAR(max), Gender VARCHAR(max),
    Dependent_Count VARCHAR(max), Education_Level VARCHAR(max), Marital_Status VARCHAR(max),
    State_Code VARCHAR(max), Zipcode VARCHAR(max), Car_Owner VARCHAR(max),
    House_Owner VARCHAR(max), Personal_Loan VARCHAR(max), Contact VARCHAR(max),
    Customer_Job VARCHAR(max), Income VARCHAR(max), Cust_Satisfaction_Score VARCHAR(max));

BULK INSERT cust_detail_staging FROM 'C:\Users\kchri\Downloads\Credit_Card_Risk\Data\customer.csv' WITH (FORMAT = 'CSV', FIRSTROW = 2, CODEPAGE = '65001', TABLOCK);

INSERT INTO cust_detail SELECT TRY_CAST(Client_Num AS INT), TRY_CAST(Customer_Age AS INT), Gender, TRY_CAST(Dependent_Count AS INT), Education_Level, Marital_Status, State_Code, Zipcode, Car_Owner, House_Owner, Personal_Loan, Contact, Customer_Job, TRY_CAST(Income AS INT), TRY_CAST(Cust_Satisfaction_Score AS INT) FROM cust_detail_staging;
DROP TABLE cust_detail_staging;
GO

-- 7. Verification
SELECT 'CC Table' as TableName, COUNT(*) FROM cc_detail
UNION ALL
SELECT 'Cust Table', COUNT(*) FROM cust_detail;
-------------------------------------------------------------------------------

USE ccdb;
GO

--------------------------------------------------------------
-- 1. PRE-CLEANUP (Removes staging tables if they already exist)
--------------------------------------------------------------
DROP TABLE IF EXISTS cc_add_staging;
DROP TABLE IF EXISTS cust_add_staging;
GO

--------------------------------------------------------------
-- 2. APPEND CREDIT CARD DATA (Week 53)
--------------------------------------------------------------
CREATE TABLE cc_add_staging (
    Client_Num VARCHAR(max), Card_Category VARCHAR(max), Annual_Fees VARCHAR(max),
    Activation_30_Days VARCHAR(max), Customer_Acq_Cost VARCHAR(max),
    Week_Start_Date VARCHAR(max), Week_Num VARCHAR(max), Qtr VARCHAR(max),
    current_year VARCHAR(max), Credit_Limit VARCHAR(max), Total_Revolving_Bal VARCHAR(max),
    Total_Trans_Amt VARCHAR(max), Total_Trans_Ct VARCHAR(max),
    Avg_Utilization_Ratio VARCHAR(max), Use_Chip VARCHAR(max), Exp_Type VARCHAR(max),
    Interest_Earned VARCHAR(max), Delinquent_Acc VARCHAR(max));

-- Using your original working C: drive path
BULK INSERT cc_add_staging 
FROM 'C:\Users\kchri\Downloads\Credit_Card_Risk\Data\cc_add.csv' 
WITH (FORMAT = 'CSV', FIRSTROW = 2, CODEPAGE = '65001', TABLOCK);

INSERT INTO cc_detail 
SELECT 
    TRY_CAST(Client_Num AS INT), Card_Category, TRY_CAST(Annual_Fees AS INT), 
    TRY_CAST(Activation_30_Days AS INT), TRY_CAST(Customer_Acq_Cost AS INT), 
    TRY_CONVERT(DATE, Week_Start_Date, 103), Week_Num, Qtr, 
    TRY_CAST(current_year AS INT), TRY_CAST(Credit_Limit AS DECIMAL(10,2)), 
    TRY_CAST(Total_Revolving_Bal AS INT), TRY_CAST(Total_Trans_Amt AS INT), 
    TRY_CAST(Total_Trans_Ct AS INT), TRY_CAST(Avg_Utilization_Ratio AS DECIMAL(10,3)), 
    Use_Chip, Exp_Type, TRY_CAST(Interest_Earned AS DECIMAL(10,3)), Delinquent_Acc 
FROM cc_add_staging;

DROP TABLE cc_add_staging;
GO

--------------------------------------------------------------
-- 3. APPEND CUSTOMER DATA (Week 53)
--------------------------------------------------------------
CREATE TABLE cust_add_staging (
    Client_Num VARCHAR(max), Customer_Age VARCHAR(max), Gender VARCHAR(max),
    Dependent_Count VARCHAR(max), Education_Level VARCHAR(max), Marital_Status VARCHAR(max),
    State_Code VARCHAR(max), Zipcode VARCHAR(max), Car_Owner VARCHAR(max),
    House_Owner VARCHAR(max), Personal_Loan VARCHAR(max), Contact VARCHAR(max),
    Customer_Job VARCHAR(max), Income VARCHAR(max), Cust_Satisfaction_Score VARCHAR(max));

BULK INSERT cust_add_staging 
FROM 'C:\Users\kchri\Downloads\Credit_Card_Risk\Data\cust_add.csv' 
WITH (FORMAT = 'CSV', FIRSTROW = 2, CODEPAGE = '65001', TABLOCK);

INSERT INTO cust_detail 
SELECT 
    TRY_CAST(Client_Num AS INT), TRY_CAST(Customer_Age AS INT), Gender, 
    TRY_CAST(Dependent_Count AS INT), Education_Level, Marital_Status, 
    State_Code, Zipcode, Car_Owner, House_Owner, Personal_Loan, Contact, 
    Customer_Job, TRY_CAST(Income AS INT), TRY_CAST(Cust_Satisfaction_Score AS INT) 
FROM cust_add_staging;

DROP TABLE cust_add_staging;
GO

--------------------------------------------------------------
-- 4. FINAL VERIFICATION
--------------------------------------------------------------
SELECT 'CC Table' as TableName, COUNT(*) FROM cc_detail
UNION ALL
SELECT 'Cust Table', COUNT(*) FROM cust_detail;
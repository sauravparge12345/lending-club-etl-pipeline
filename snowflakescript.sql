-- STEP 1: SET UP THE ENVIRONMENT
CREATE DATABASE IF NOT EXISTS lending_club_db;
CREATE WAREHOUSE IF NOT EXISTS lending_club_wh WITH WAREHOUSE_SIZE = 'XSMALL';

USE WAREHOUSE lending_club_wh;
USE DATABASE lending_club_db;
USE SCHEMA PUBLIC;

-- STEP 2: CREATE THE TABLE
CREATE OR REPLACE TABLE lending_club_loans (
    id VARCHAR(50),
    loan_amnt FLOAT,
    term INT,
    int_rate FLOAT,
    installment FLOAT,
    grade VARCHAR(5),
    emp_length VARCHAR(20),
    home_ownership VARCHAR(20),
    annual_inc FLOAT,
    loan_status VARCHAR(50)
);

-- STEP 3: LOAD THE DATA FROM S3

COPY INTO lending_club_db.public.lending_club_loans
FROM 's3://pipelinebigdata/lending_club/clean/2026-01-23/lending_club_cleaned.parquet'
CREDENTIALS = (
    AWS_KEY_ID 
    AWS_SECRET_KEY 
)
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE; 

-- STEP 4: VERIFY AND ANALYZE
SELECT * FROM lending_club_db.public.lending_club_loans LIMIT 10;

SELECT * FROM lending_club_loans LIMIT 10;

SELECT COUNT(*) as total_clean_rows FROM lending_club_loans;

-- Calculate Default Risk by Loan Grade
SELECT 
    grade,
    COUNT(id) as total_loans,
    AVG(int_rate) as avg_interest_rate,
    SUM(CASE WHEN loan_status IN ('Charged Off', 'Default') THEN 1 ELSE 0 END) / COUNT(id) * 100 as default_rate_percentage
FROM 
    lending_club_loans
GROUP BY 
    grade
ORDER BY 
    grade ASC;




    -- 
DROP TABLE lending_club_db.public.lending_club_loans;
DROP DATABASE lending_club_db;
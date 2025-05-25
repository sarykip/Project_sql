create database customers_transaction;

UPDATE customers set Gender = null where Gender = '';
UPDATE customers set Age = null where Age = '';
ALTER TABLE Customers modify Age int null;

select * from customers;

create table Transactions
(date_new DATE,
Id_check INT,
ID_client INT,
Count_products DECIMAL(10,3),
Sum_payment DECIMAL (10,2));

LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\TRANSACTIONS.csv"
INTO TABLE Transactions
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM TRANSACTIONS;




# 1 ЗАДАНИЕ

SELECT 
    t.ID_client,
    COUNT(*) AS total_transactions,
    SUM(t.Sum_payment) / COUNT(*) AS avg_check,
    SUM(t.Sum_payment) / 12 AS avg_monthly_spending,
    SUM(t.Sum_payment) AS total_spending
FROM transactions t
WHERE t.date_new >= '2015-06-01' AND t.date_new < '2016-06-01'
GROUP BY t.ID_client
HAVING COUNT(DISTINCT DATE_FORMAT(t.date_new, '%Y-%m')) = 12;

# 2 ЗАДАНИЕ
	# средняя сумма чека в месяц;

SELECT 
    DATE_FORMAT(date_new, '%Y-%m') AS month,
    COUNT(*) AS transactions_count,
    SUM(Sum_payment) AS total_amount,
    ROUND(SUM(Sum_payment) / COUNT(*), 2) AS avg_check
FROM transactions
WHERE date_new BETWEEN '2015-06-01' AND '2016-06-01'
GROUP BY DATE_FORMAT(date_new, '%Y-%m')
ORDER BY month;

	# среднее количество операций в месяц;
SELECT 
    ID_client,
    COUNT(*) AS total_transactions,
    ROUND(COUNT(*) / 12, 2) AS avg_transactions_per_month
FROM transactions
WHERE date_new BETWEEN '2015-06-01' AND '2016-06-01'
GROUP BY ID_client
HAVING COUNT(DISTINCT DATE_FORMAT(date_new, '%Y-%m')) = 12
ORDER BY avg_transactions_per_month DESC;

	# среднее количество клиентов, которые совершали операции;
SELECT 
    ROUND(AVG(client_count), 2) AS avg_clients_per_month
FROM (
    SELECT 
        DATE_FORMAT(date_new, '%Y-%m') AS month,
        COUNT(DISTINCT ID_client) AS client_count
    FROM transactions
    WHERE date_new BETWEEN '2015-06-01' AND '2016-06-01'
    GROUP BY DATE_FORMAT(date_new, '%Y-%m')
) AS monthly_clients;

	# долю от общего количества операций за год и долю в месяц от общей суммы операций;
    SELECT 
    DATE_FORMAT(date_new, '%Y-%m') AS month,
    COUNT(*) AS transactions_in_month,
    ROUND(COUNT(*) / total_yearly_transactions, 4) AS transaction_share, -- Доля операций
    SUM(Sum_payment) AS total_sum_in_month,
    ROUND(SUM(Sum_payment) / total_yearly_sum, 4) AS sum_share           -- Доля по сумме
FROM transactions,
    (
        SELECT 
            COUNT(*) AS total_yearly_transactions,
            SUM(Sum_payment) AS total_yearly_sum
        FROM transactions
        WHERE date_new BETWEEN '2015-06-01' AND '2016-06-01'
    ) AS totals
WHERE date_new BETWEEN '2015-06-01' AND '2016-06-01'
GROUP BY DATE_FORMAT(date_new, '%Y-%m')
ORDER BY month;

	# вывести % соотношение M/F/NA в каждом месяце с их долей затрат;
SELECT 
    DATE_FORMAT(t.date_new, '%Y-%m') AS month,
    c.Gender,
    COUNT(DISTINCT t.ID_client) AS client_count,
    ROUND(COUNT(DISTINCT t.ID_client) * 100.0 / SUM(COUNT(DISTINCT t.ID_client)) OVER (PARTITION BY DATE_FORMAT(t.date_new, '%Y-%m')), 2) AS client_share_pct,
    SUM(t.Sum_payment) AS total_sum,
    ROUND(SUM(t.Sum_payment) * 100.0 / SUM(SUM(t.Sum_payment)) OVER (PARTITION BY DATE_FORMAT(t.date_new, '%Y-%m')), 2) AS sum_share_pct
FROM transactions t
JOIN customers c ON t.ID_client = c.ID_client
WHERE t.date_new BETWEEN '2015-06-01' AND '2016-06-01'
GROUP BY DATE_FORMAT(t.date_new, '%Y-%m'), c.Gender
ORDER BY month, c.Gender;


# 3 ЗАДАНИЕ



select * from customers;
select * from transactions;


-- 1. Создаем представление для объединения таблиц
CREATE OR REPLACE VIEW transactions_with_age AS
SELECT
    t.ID_client,
    t.date_new,
    QUARTER(t.date_new) AS quarter,
    t.Sum_payment,
    c.Age
FROM transactions t
LEFT JOIN customers c ON t.ID_client = c.ID_client;

-- 2. Итоговый запрос с возрастными группами
SELECT
    age_group,
    COUNT(*) AS transaction_count,
    SUM(Sum_payment) AS total_sum,
    
    ROUND(AVG(CASE WHEN quarter = 1 THEN Sum_payment END), 2) AS avg_q1,
    ROUND(AVG(CASE WHEN quarter = 2 THEN Sum_payment END), 2) AS avg_q2,
    ROUND(AVG(CASE WHEN quarter = 3 THEN Sum_payment END), 2) AS avg_q3,
    ROUND(AVG(CASE WHEN quarter = 4 THEN Sum_payment END), 2) AS avg_q4,

    ROUND(100 * SUM(Sum_payment) / (
        SELECT SUM(Sum_payment) FROM transactions_with_age
    ), 2) AS percent_of_total
FROM (
    SELECT *,
        CASE
            WHEN Age IS NULL THEN 'Unknown'
            WHEN Age < 10 THEN '00-09'
            WHEN Age BETWEEN 10 AND 19 THEN '10-19'
            WHEN Age BETWEEN 20 AND 29 THEN '20-29'
            WHEN Age BETWEEN 30 AND 39 THEN '30-39'
            WHEN Age BETWEEN 40 AND 49 THEN '40-49'
            WHEN Age BETWEEN 50 AND 59 THEN '50-59'
            WHEN Age BETWEEN 60 AND 69 THEN '60-69'
            WHEN Age BETWEEN 70 AND 79 THEN '70-79'
            WHEN Age BETWEEN 80 AND 89 THEN '80-89'
            ELSE '90+'
        END AS age_group
    FROM transactions_with_age
) AS grouped
GROUP BY age_group
ORDER BY age_group;

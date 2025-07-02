-- Create Table dim_product
CREATE TABLE dim_product (
    product_key VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    brand VARCHAR(255),
    category VARCHAR(255),
    department VARCHAR(255),
    retail_price DECIMAL(10,2),
    cost DECIMAL(10,2)
);

-- Create Table dim_user
CREATE TABLE dim_user (
    user_key VARCHAR(255) PRIMARY KEY,
    customer_name VARCHAR(255),
    age INTEGER,
    gender VARCHAR(50),
    state VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(100),
    traffic_source VARCHAR(100)
);

-- Create Table dim_date
CREATE TABLE dim_date (
    date_key VARCHAR(8) PRIMARY KEY,
    full_date DATE,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    week INTEGER,
    day INTEGER,
    month_name VARCHAR(20),
    day_name VARCHAR(20),
    is_weekend BOOLEAN
);

-- Create Table fact transaction
CREATE TABLE fact_transaction (
    transaction_id VARCHAR(255) PRIMARY KEY,
    order_id VARCHAR(255),
    user_id VARCHAR(255),
    product_id VARCHAR(255),
    date_id VARCHAR(8),
    sale_price DECIMAL(10,2),
    cost DECIMAL(10,2),
    profit DECIMAL(10,2),
    transaction_status VARCHAR(50),
    is_order_created BOOLEAN,
    is_shipped BOOLEAN,
    is_delivered BOOLEAN,
    is_returned BOOLEAN,
    num_of_item INTEGER,
    FOREIGN KEY (product_id) REFERENCES dim_product(product_key),
    FOREIGN KEY (user_id) REFERENCES dim_user(user_key),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_key)
);

-- Check the number of rows to ensure it matches with PySpark
SELECT COUNT(*) FROM dim_user;
SELECT COUNT(*) FROM dim_product;
SELECT COUNT(*) FROM dim_date;
SELECT COUNT(*) FROM fact_transaction;


-- Analysis
-- 1. Produk dengan profit tertinggi
SELECT 
    dp.name AS product_name,
    SUM(ft.profit) AS total_profit
FROM fact_transaction ft
JOIN dim_product dp ON ft.product_id = dp.product_key
GROUP BY dp.name
ORDER BY total_profit DESC
LIMIT 1;


--2. Negara dengan transaksi terbanyak
SELECT 
    du.country,
    COUNT(ft.transaction_id) AS total_transactions
FROM fact_transaction ft
JOIN dim_user du ON ft.user_id = du.user_key
GROUP BY du.country
ORDER BY total_transactions DESC
LIMIT 1;

-- 3. Tren penjualan bulanan (setahun terakhir)
SELECT 
    dd.year,
    dd.month,
    dd.month_name,
    SUM(ft.sale_price) AS total_sales
FROM fact_transaction ft
JOIN dim_date dd ON ft.date_id = dd.date_key
WHERE dd.year = EXTRACT(YEAR FROM CURRENT_DATE)
GROUP BY dd.year, dd.month, dd.month_name
ORDER BY dd.month;

-- 4. Rata-rata profit per-transaksi tiap gender
SELECT 
    du.gender,
    ROUND(AVG(ft.profit), 2) AS avg_profit_per_transaction
FROM fact_transaction ft
JOIN dim_user du ON ft.user_id = du.user_key
GROUP BY du.gender;

-- 5. Banyaknya produk yang di-return
SELECT 
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN ft.is_returned THEN 1 ELSE 0 END) AS returned_transactions,
    ROUND(SUM(CASE WHEN ft.is_returned THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS return_rate_percentage
FROM fact_transaction ft;

-- 6. Produk yang paling banyak di-return
SELECT 
    dp.name AS product_name,
    COUNT(ft.transaction_id) AS total_transactions,
    SUM(CASE WHEN ft.is_returned THEN 1 ELSE 0 END) AS returned_transactions,
    ROUND(SUM(CASE WHEN ft.is_returned THEN 1 ELSE 0 END) * 100.0 / COUNT(ft.transaction_id), 2) AS return_rate_percentage
FROM fact_transaction ft
JOIN dim_product dp ON ft.product_id = dp.product_key
GROUP BY dp.name
HAVING COUNT(ft.transaction_id) > 10  -- untuk menghindari produk dengan transaksi terlalu sedikit
ORDER BY return_rate_percentage DESC
LIMIT 5;




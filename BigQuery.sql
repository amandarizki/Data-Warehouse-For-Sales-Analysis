-- query data for fact_transaction
SELECT 
  oi.id AS transaction_id,
  oi.order_id,
  oi.user_id,
  oi.product_id,
  FORMAT_DATE('%Y%m%d', DATE(oi.created_at)) AS date_id,
  oi.sale_price,
  p.cost,
  (oi.sale_price - p.cost) AS profit,
  oi.status AS transaction_status,
  o.order_id IS NOT NULL AS is_order_created,
  oi.shipped_at IS NOT NULL AS is_shipped,
  oi.delivered_at IS NOT NULL AS is_delivered,
  oi.returned_at IS NOT NULL AS is_returned,
  o.num_of_item
FROM bigquery-public-data.thelook_ecommerce.order_items oi
LEFT JOIN bigquery-public-data.thelook_ecommerce.products p ON oi.product_id = p.id
LEFT JOIN bigquery-public-data.thelook_ecommerce.orders o ON oi.order_id = o.order_id;

-- query data for dim_product
SELECT 
  id AS product_key,
  name,
  brand,
  category,
  department,
  retail_price,
  cost
FROM bigquery-public-data.thelook_ecommerce.products;

-- query data for dim_user
SELECT 
  id AS user_key,
  first_name || ' ' || last_name AS customer_name,
  age,
  gender,
  state,
  city,
  country,
  traffic_source
FROM bigquery-public-data.thelook_ecommerce.users;

-- query data for dim_date
WITH unique_dates AS (
  SELECT DISTINCT DATE(created_at) AS transaction_date
  FROM `bigquery-public-data.thelook_ecommerce.order_items`
)
SELECT
  FORMAT_DATE('%Y%m%d', transaction_date) AS date_key,
  transaction_date AS full_date,
  EXTRACT(YEAR FROM transaction_date) AS year,
  EXTRACT(QUARTER FROM transaction_date) AS quarter,
  EXTRACT(MONTH FROM transaction_date) AS month,
  EXTRACT(WEEK FROM transaction_date) AS week,
  EXTRACT(DAY FROM transaction_date) AS day,
  FORMAT_DATE('%B', transaction_date) AS month_name,
  FORMAT_DATE('%A', transaction_date) AS day_name,
  CASE 
    WHEN EXTRACT(DAYOFWEEK FROM transaction_date) IN (1, 7) THEN TRUE 
    ELSE FALSE 
  END AS is_weekend
FROM unique_dates
ORDER BY transaction_date;
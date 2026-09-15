DROP TABLE IF EXISTS daily_sales;
CREATE TABLE daily_sales AS
SELECT DATE(order_date) AS day, SUM(amount) AS total
FROM orders
GROUP BY DATE(order_date);
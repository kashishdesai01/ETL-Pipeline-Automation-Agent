-- Main ETL Pipeline
SELECT 
    order_id, 
    product_id, 
    amount AS original_price,
    calculate_discount(amount, 15) AS discounted_price,
    SYSDATE AS processing_time
FROM orders
WHERE status = 'active';

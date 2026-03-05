-- mysql_001.sql
-- Legacy MySQL ETL: Product sales by category with rolling average
-- Uses: MySQL date functions, GROUP_CONCAT, backtick quoting, non-standard LIMIT

SELECT
    p.`category`,
    COUNT(DISTINCT o.`id`)                             AS total_orders,
    SUM(li.`quantity` * li.`unit_price`)               AS total_revenue,
    AVG(li.`unit_price`)                               AS avg_price,
    GROUP_CONCAT(DISTINCT p.`name` ORDER BY p.`name` SEPARATOR ', ')  AS products_list
FROM `orders` o
    INNER JOIN `line_items` li  ON li.`order_id` = o.`id`
    INNER JOIN `products` p     ON p.`id` = li.`product_id`
WHERE o.`order_date` >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
  AND o.`status` = 'active'
GROUP BY p.`category`
HAVING total_revenue > 5000
ORDER BY total_revenue DESC
LIMIT 20;

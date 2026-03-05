-- oracle_002.sql
-- Legacy Oracle ETL: Customer LTV with ranked window function using ROWNUM workaround
-- Uses: subqueries instead of CTEs, ROWNUM pagination, NVL2, Oracle date arithmetic

SELECT customer_id,
       customer_name,
       total_spend,
       avg_order_value,
       order_count,
       revenue_rank
FROM (
    SELECT c.id                                          AS customer_id,
           NVL(c.name, 'Unknown')                        AS customer_name,
           NVL(SUM(o.amount), 0)                         AS total_spend,
           NVL(AVG(o.amount), 0)                         AS avg_order_value,
           COUNT(o.id)                                   AS order_count,
           RANK() OVER (ORDER BY SUM(o.amount) DESC)     AS revenue_rank
    FROM   customers c,
           orders o
    WHERE  c.id       = o.customer_id
    AND    o.order_date BETWEEN TRUNC(SYSDATE, 'YEAR') AND SYSDATE
    AND    o.status   != 'inactive'
    GROUP  BY c.id, c.name
)
WHERE revenue_rank <= 100
ORDER BY revenue_rank;

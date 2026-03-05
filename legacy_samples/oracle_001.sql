-- oracle_001.sql
-- Legacy Oracle ETL: Rolling 30-day GMV by region with rank filter
-- Uses Oracle-specific: NVL, TRUNC, implicit joins, ROWNUM

SELECT region,
       NVL(SUM(o.amount), 0)     AS total_gmv,
       COUNT(o.id)                AS order_count,
       TRUNC(SYSDATE)             AS run_date
FROM   orders o,
       customers c,
       regions r
WHERE  o.customer_id  = c.id
AND    c.region       = r.name
AND    o.order_date  >= ADD_MONTHS(TRUNC(SYSDATE), -1)
AND    o.status       = 'active'
GROUP  BY region
HAVING SUM(o.amount) > 1000
ORDER  BY total_gmv DESC;

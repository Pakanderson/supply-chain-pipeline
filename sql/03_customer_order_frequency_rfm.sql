-- 03_customer_order_frequency_rfm.sql
--
-- The same RFM technique used in the e-commerce project (sql/ in the
-- sibling repo), applied to this domain — showing the technique
-- transfers, not just the query text. Recency/Frequency/Monetary
-- segmentation of customers by order behavior and spend.
--
-- Interview talking point: NTILE(4) here instead of NTILE(5) — with
-- ~20k customers this is a judgment call on how many buckets are
-- meaningful vs. how much each bucket actually differs from its
-- neighbors. Being able to justify a bucket-count choice (not just use a
-- default) is worth being ready to explain.

WITH customer_orders AS (
    SELECT
        c.customer_key,
        c.segment,
        f.order_id,
        MAX(d.full_date) AS order_date,
        SUM(f.order_item_total) AS order_value
    FROM fact_order_item f
    JOIN dim_customer c ON c.customer_key = f.customer_key
    JOIN dim_date d ON d.date_key = f.order_date_key
    GROUP BY c.customer_key, c.segment, f.order_id
),
rfm_base AS (
    SELECT
        customer_key,
        segment,
        CAST(julianday((SELECT MAX(order_date) FROM customer_orders))
             - julianday(MAX(order_date)) AS INTEGER) AS recency_days,
        COUNT(DISTINCT order_id) AS frequency,
        SUM(order_value) AS monetary
    FROM customer_orders
    GROUP BY customer_key, segment
),
rfm_scored AS (
    SELECT
        customer_key,
        segment,
        recency_days,
        frequency,
        monetary,
        5 - NTILE(4) OVER (ORDER BY recency_days ASC) AS recency_score,
        NTILE(4) OVER (ORDER BY frequency ASC)         AS frequency_score,
        NTILE(4) OVER (ORDER BY monetary ASC)          AS monetary_score
    FROM rfm_base
)
SELECT
    segment,
    CASE
        WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Top customers'
        WHEN recency_score <= 2 AND frequency_score <= 2 THEN 'Churn risk'
        ELSE 'Mid-tier'
    END AS rfm_tier,
    COUNT(*) AS customers,
    ROUND(AVG(monetary), 2) AS avg_lifetime_value
FROM rfm_scored
GROUP BY segment, rfm_tier
ORDER BY segment, rfm_tier;

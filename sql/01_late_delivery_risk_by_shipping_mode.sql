-- 01_late_delivery_risk_by_shipping_mode.sql
--
-- For each shipping mode and region, what fraction of orders were flagged
-- as a late-delivery risk, and how does the actual vs. scheduled shipping
-- time compare? This is the headline "is our logistics network healthy"
-- query for an operations team.
--
-- Interview talking point: two CTEs chained together (the second reads
-- from the first) — a good pattern once a query needs more than one
-- logical step, because each CTE can be understood (and tested) on its
-- own instead of one deeply nested query.

WITH base AS (
    SELECT
        f.shipping_mode,
        l.region,
        f.late_delivery_risk,
        f.days_for_shipping_real,
        f.days_for_shipment_scheduled,
        (f.days_for_shipping_real - f.days_for_shipment_scheduled) AS days_over_schedule
    FROM fact_order_item f
    JOIN dim_location l ON l.location_key = f.location_key
    WHERE f.days_for_shipping_real IS NOT NULL
),
by_mode_region AS (
    SELECT
        shipping_mode,
        region,
        COUNT(*) AS orders,
        SUM(late_delivery_risk) AS flagged_risky,
        ROUND(100.0 * SUM(late_delivery_risk) / COUNT(*), 1) AS risky_pct,
        ROUND(AVG(days_over_schedule), 2) AS avg_days_over_schedule
    FROM base
    GROUP BY shipping_mode, region
)
SELECT *
FROM by_mode_region
WHERE orders >= 30
ORDER BY risky_pct DESC
LIMIT 25;

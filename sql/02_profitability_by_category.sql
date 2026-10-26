-- 02_profitability_by_category.sql
--
-- Which product categories/departments actually make money, and which
-- ones sell a lot but barely break even (or lose money) once discounts
-- are factored in? RANK() surfaces the best and worst within each
-- department side by side.
--
-- Interview talking point: this deliberately reports BOTH total profit
-- AND profit margin (profit / sales) — a category can rank #1 on raw
-- profit purely because it sells a huge volume while actually having a
-- thin (or negative) margin. Reporting only one of the two is a classic
-- way to accidentally mislead a stakeholder.

WITH category_performance AS (
    SELECT
        p.department_name,
        p.category_name,
        COUNT(*) AS line_items,
        SUM(f.sales) AS total_sales,
        SUM(f.profit_per_order) AS total_profit,
        ROUND(SUM(f.profit_per_order) / NULLIF(SUM(f.sales), 0) * 100, 1) AS margin_pct
    FROM fact_order_item f
    JOIN dim_product p ON p.product_key = f.product_key
    GROUP BY p.department_name, p.category_name
),
ranked AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY department_name ORDER BY total_profit DESC) AS profit_rank_in_department
    FROM category_performance
)
SELECT
    department_name,
    category_name,
    line_items,
    ROUND(total_sales, 2) AS total_sales,
    ROUND(total_profit, 2) AS total_profit,
    margin_pct,
    profit_rank_in_department
FROM ranked
WHERE profit_rank_in_department <= 3 OR margin_pct < 0
ORDER BY department_name, profit_rank_in_department;

-- file: sql/elb_cost_last_month.sql
WITH last_full_month AS (
  SELECT
    date_trunc('month', current_date - interval '1' month) AS start_dt,
    date_trunc('month', current_date) AS end_dt
),
cur AS (
  SELECT
    line_item_resource_id,
    line_item_usage_account_id,
    product_region,
    SUM(line_item_usage_amount)               AS usage_qty,
    SUM(line_item_unblended_cost)             AS unblended_cost
  FROM ${athena_db}.${athena_table}, last_full_month
  WHERE line_item_product_code = 'AWSELB'
    AND CAST(from_iso8601_timestamp(CONCAT(year,'-',month,'-01T00:00:00Z')) AS date)
        >= (SELECT start_dt FROM last_full_month)
    AND CAST(from_iso8601_timestamp(CONCAT(year,'-',month,'-01T00:00:00Z')) AS date)
        <  (SELECT end_dt   FROM last_full_month)
    AND line_item_line_item_type IN ('Usage','DiscountedUsage')
  GROUP BY line_item_resource_id, line_item_usage_account_id, product_region
)
SELECT
  line_item_usage_account_id   AS "Account Id",
  product_region               AS "Region",
  line_item_resource_id        AS "Resource Id",
  usage_qty                    AS "Monthly Usage Hours",
  CAST(ROUND(unblended_cost * 12) AS BIGINT) AS "Annual Saving",
  unblended_cost               AS "Monthly Cost"
FROM cur
ORDER BY unblended_cost DESC;

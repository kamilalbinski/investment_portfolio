DROP VIEW IF EXISTS AGGREGATED_PORTFOLIO_VALUES;

CREATE VIEW AGGREGATED_PORTFOLIO_VALUES AS

WITH daily_portfolio_values AS (
    SELECT
        av.TIMESTAMP,
        av.ACCOUNT_OWNER,
        a.SUB_CATEGORY,
        SUM(av.AGGREGATED_VALUE) AS AGGREGATED_VALUE
    FROM
        AGGREGATED_VALUES av
    JOIN
        ASSETS a ON av.ASSET_ID = a.ASSET_ID
    GROUP BY
        av.TIMESTAMP, av.ACCOUNT_OWNER, a.SUB_CATEGORY
),
total_values AS (
    SELECT
        TIMESTAMP,
        'None' AS ACCOUNT_OWNER,
        SUB_CATEGORY,
        SUM(AGGREGATED_VALUE) AS AGGREGATED_VALUE
    FROM
        daily_portfolio_values
    GROUP BY
        TIMESTAMP, SUB_CATEGORY
)
SELECT *
FROM daily_portfolio_values
UNION ALL
SELECT *
FROM total_values
ORDER BY TIMESTAMP, ACCOUNT_OWNER, SUB_CATEGORY;
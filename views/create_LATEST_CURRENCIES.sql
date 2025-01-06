DROP VIEW IF EXISTS LATEST_CURRENCIES;

CREATE VIEW LATEST_CURRENCIES AS
SELECT
    c.ASSET_ID,
    c.DATE,
    c.PRICE
FROM
    CURRENCIES c
WHERE
    c.DATE = (
        SELECT
            MAX(sub_c.DATE)
        FROM
            CURRENCIES sub_c
        WHERE
            sub_c.ASSET_ID = c.ASSET_ID
    );
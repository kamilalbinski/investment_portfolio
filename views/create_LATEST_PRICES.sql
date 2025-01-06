DROP VIEW IF EXISTS LATEST_PRICES;

CREATE VIEW LATEST_PRICES AS
SELECT
    p.ASSET_ID,
    p.DATE,
    p.PRICE
FROM
    PRICES p
WHERE
    p.DATE = (
        SELECT
            MAX(sub_p.DATE)
        FROM
            PRICES sub_p
        WHERE
            sub_p.ASSET_ID = p.ASSET_ID
    );
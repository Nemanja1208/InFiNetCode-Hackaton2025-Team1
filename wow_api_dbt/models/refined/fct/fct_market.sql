-- models/refined/fct/fct_market.sql

-- one fct table to rule them all
-- this table will contain all the market data, including auctions, commodities, realms and items by joining all dim tables
-- Only containing ids from the dim tables, no need to store the full data again
WITH market_data AS (
    SELECT
        a.id AS auction_id,
        i.id AS item_id,
        --c.id AS commodity_id,
        r.connected_realm_id AS connected_realm_id,
        r.id as realm_id
    FROM {{ ref('dim_auctions') }} AS a
    LEFT JOIN {{ ref('dim_items')}} AS i ON i.id = a.item_id
    --JOIN {{ ref('dim_commodities')}} AS c ON c.item_id = i.id
    LEFT JOIN {{ ref('dim_realms')}} AS r ON r.connected_realm_id = a.realm_id
)

SELECT * FROM market_data
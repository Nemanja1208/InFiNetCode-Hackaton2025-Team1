-- models/dim/dim_realms.sql

WITH connected_realms AS (
  SELECT 
    realm_data.id AS connected_realm_id,
    rdr.id AS id,
    rdr.name__en_us AS name,
    rdr.category__en_us AS category,
    rdr.timezone,
    rdr.type__name__en_us AS type,
    realm_data.has_queue,
    realm_data.status__name__en_us AS server_status,
    realm_data.population__name__en_us AS population_status,
    realm_data.mythic_leaderboards__href,
    realm_data.auctions__href AS auctions_href,
    rdr.region__name__en_us AS region_name,
    rdr.region__id AS region_id,
    rdr.is_tournament
  FROM {{ source('raw', 'realm_data') }} AS realm_data
  JOIN {{ source('raw', 'realm_data__realms') }} AS rdr
    ON rdr._dlt_parent_id = realm_data._dlt_id
)

SELECT *
FROM connected_realms

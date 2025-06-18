-- models/refined/dim/dim_auctions.sql

SELECT
  id,
  item__id AS item_id,
  bid,
  buyout,
  quantity,
  time_left,
  item__context AS item_context,
  item__pet_breed_id AS item_pet_breed_id,
  item__pet_level AS item_pet_level,
  item__pet_quality_id AS item_pet_quality_id,
  item__pet_species_id AS item_pet_species,
  _dlt_load_id,
  _dlt_id,
  realm_id
FROM {{ source('raw', 'auctions') }}


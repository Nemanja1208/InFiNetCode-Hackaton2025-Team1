-- models/refined/dim/dim_items.sql

SELECT
    id,
    level,
    required_level,
    sell_price,
    item_subclass__name__en_us AS item_subclass_name,
    item_subclass__id AS item_subclass_id,
    is_stackable,
    is_equippable,
    purchase_quantity,
    media__id AS media_id,
    item_class__name__en_us AS item_class_name,
    item_class__id AS item_class_id,
    quality__name__en_us AS quality_name,
    quality__type AS quality_type,
    max_count,
    name__en_us AS name,
    purchase_price,
    inventory_type__name__en_us AS inventory_type,
    _dlt_load_id,
    _dlt_id
FROM {{ source('raw', 'items') }}

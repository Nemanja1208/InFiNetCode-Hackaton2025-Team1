-- models/marts/mart_weapon_details.sql


WITH mart_weapon_details AS (
    SELECT
		did.id,
        did.weapon_damage,
        did.attack_speed,
        did.dps,
        did.stat_type,
        did.value

FROM {{ ref('dim_item_details') }} AS did
    )

SELECT * FROM mart_weapon_details
"""
All Streamlit UI functions for main content (item/auction details, main tables, etc).
- Anything that is a "main page" or "main section" component.
"""

import streamlit as st
import pandas as pd
from .helpers import get_rarity_color

# ---------- Items page components ----------
def render_item_details(item: dict, item_details: dict):
    # Item detail variables
    item_name = item.get("item_name")
    item_class = item.get("item_class_name")
    item_subclass = item.get("item_subclass_name")
    item_type = item.get("item_type")
    item_ilvl = item.get("item_level")
    item_req_lvl = item.get("required_level")

    # Get weapon attributes
    if not item_details.empty:
        weapon_damage = item_details.iloc[0]["weapon_damage"]
        weapon_speed = item_details.iloc[0]["attack_speed"]
        weapon_dps = item_details.iloc[0]["dps"]
    else:
        weapon_damage = weapon_speed = weapon_dps = None

    # Get all stat rows (stat_type + value)
    stats = []
    if not item_details.empty:
        stats = [
            {"stat_type": row["stat_type"], "value": row["value"]}
            for _, row in item_details.iterrows()
            if pd.notnull(row["stat_type"]) and pd.notnull(row["value"])
        ]

    # Retrieve item rarity and assign color to variable
    rarity = item.get("rarity_name", "Common")
    color = get_rarity_color(rarity) # Defaults to white
    
    # ---------- Item details layout ----------
    st.markdown(f"<span style='font-size:1em;'>{item_class} → {item_subclass} → {item_type}</span>", unsafe_allow_html=True)
    image_col, name_col = st.columns([0.08, 0.92])

    # Image column (10% width)
    with image_col:
        image_url = item.get("icon_href", "https://wow.zamimg.com/images/wow/icons/large/inv_misc_questionmark.jpg")
        st.image(
            image = image_url,
            width = 56
        )

    # Item details column (90% width)
    with name_col:
        # Layout/details
        st.header(f"{item_name}")

    rarity_col, ilvl_col, reqlvl_col = st.columns([1, 1, 1])

    # Rarity
    with rarity_col:
        with st.container(border=True):
            st.markdown(
                f"<span style='background:{color};color:#222;padding:6px 18px;border-radius:16px;font-weight:bold;font-size:1em;'>{rarity}</span> ",
                unsafe_allow_html=True
            )            

    # Item level
    with ilvl_col:
        with st.container(border=True):
            st.markdown(f"<span style='font-size:1em;'>iLevel: {item_ilvl}</span>", unsafe_allow_html=True)

    # Required level
    with reqlvl_col:
        with st.container(border=True):
            st.markdown(f"<span style='font-size:1em;'>Req. level: {item_req_lvl}</span>", unsafe_allow_html=True)
    
    attributes_col, stats_col = st.columns([1, 1])

    # Attributes column (left)
    with attributes_col:
        with st.container(border=True):
            st.subheader("Attributes:")
            st.markdown(f"{weapon_damage}")
            st.markdown(f"{weapon_speed}")
            st.markdown(f"{weapon_dps}")

    # Stats column (right)
    with stats_col:
        with st.container(border=True):
            st.subheader("Stats:")
            for stat in stats:
                stat_type = stat.get("stat_type", "Unknown")
                value = stat.get("value", 0)
                st.markdown(f"{stat_type}: {value}")

    # Description column (bottom)
    with st.container(border=True):
        st.subheader("Description:")
        # Placeholder description
        item_description = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nulla vitae ipsum pharetra metus mollis gravida. Etiam vestibulum augue egestas aliquet efficitur. Pellentesque placerat odio quis lacinia elementum."
        st.markdown(item_description)

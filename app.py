import streamlit as st
import pandas as pd
import duckdb

from pipeline import create_pipeline, wow_api_source
from dotenv import load_dotenv

load_dotenv()
# Streamlit app to display World of Warcraft Auction House data
st.title("World of Warcraft Auction House Data")

# Create the DLT pipeline
pipeline = create_pipeline()

# Run the pipeline
pipeline.run(wow_api_source())

# Query the data from DuckDB
con = duckdb.connect(pipeline.pipeline_name + ".duckdb")

auctions_query = """
    SELECT
        a.id,
        i.name__en_us,
        a.buyout,
        a.quantity,
        a.time_left
    FROM raw.wow_auctions a
    JOIN raw.wow_items i
        ON a.item__id = i.id
"""

items_query = """
    SELECT
        id,
        level AS item_level,
        required_level,
        sell_price,
        item_class__name__en_us AS class,
        item_subclass__name__en_us AS subclass,
        name__en_us AS name,
        quality__name__en_us AS rarity,
    FROM raw.wow_items
"""

item_classes_query = """
    SELECT
        name,
    FROM raw.wow_item_class_indexes
"""

auctions_df = con.execute(auctions_query).df()
items_df = con.execute(items_query).df()
item_classes_df = con.execute(item_classes_query).df()
con.close()

# Display the data in Streamlit
st.subheader("Auctions RAW Data")
if auctions_df.empty:
    st.write("No data available.")
else:
    st.dataframe(auctions_df.head(50), hide_index=True)
    st.write("Total Auctions:", len(auctions_df))

st.subheader("Items RAW Data")
if items_df.empty:
    st.write("No data available.")
else:
    st.dataframe(items_df.head(50), hide_index=True)
    st.write("Total Items:", len(items_df))

st.subheader("Item Classes")
if item_classes_df.empty:
    st.write("No data available.")
else:
    st.dataframe(item_classes_df.head(50), hide_index=True)
    st.write("Total item classes:", len(item_classes_df))
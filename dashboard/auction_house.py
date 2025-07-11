import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
from .utils import fetch_data_from_db, get_sidebar_filters, build_auctions_query_conditions, render_active_filters
from .helpers import get_rarity_color, format_auction_listings, format_wow_currency
from .main_components import render_item_details
from .sidebar_components import free_text_search

# ---------- Items Page ----------
def auction_house_page():
    st.title("Auction House")

    search_column, filters_column = st.columns([0.35, 0.65])

    with search_column:
        free_text_search()
    
    with filters_column:
        st.markdown("<div style='height: 1.75em'></div>", unsafe_allow_html=True)
        filters = get_sidebar_filters()
        render_active_filters(filters)

    # Build conditions + clause based on current filters
    conditions = build_auctions_query_conditions(filters)
    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    # Fetch ALL filtered data - currently limited due to performance reasons
    auctions_query = f"""
        SELECT
            item_id,
            MAX(media_id) AS media_id,
            item_name,
            MAX(item_class_name) AS item_class_name,
            MAX(item_subclass_name) AS item_subclass_name,
            MAX(rarity_name) AS rarity_name,
            MAX(item_level) AS item_level,
            MAX(required_level) AS required_level,
            MAX(icon_href) AS icon_href,
            COUNT(DISTINCT auction_id) AS auction_count,
            MIN(buyout) AS min_buyout,
            MAX(buyout) AS max_buyout
        FROM refined.mart_market
        {where_clause}
        GROUP BY item_id, item_name
        ORDER BY item_id ASC
    """
    auctions_data = fetch_data_from_db(query=auctions_query)

    # Use icon_href directly, with fallback if missing
    auctions_data["icon_href"] = auctions_data["icon_href"].fillna("https://wow.zamimg.com/images/wow/icons/large/inv_misc_questionmark.jpg")

    # Add rarity color to dataframe
    auctions_data["rarity_color"] = auctions_data["rarity_name"].map(get_rarity_color)

    # Results column + details column (50/50%)
    col_results, col_auction_details, col_item_details = st.columns([0.35, 0.25, 0.4])

    # ---------- Results dataframe column ----------
    with col_results:
        with st.container(border=True):
            st.subheader("Auctions")
            if not auctions_data.empty:
                all_display_data = auctions_data[[
                    "icon_href", "item_name", "min_buyout", "auction_count", 
                    "item_id", "rarity_color", "rarity_name"
                ]].copy()
                all_display_data["Price_Formatted"] = all_display_data["min_buyout"].apply(format_wow_currency)
                all_display_data["price_color"] = "#FFD700"

                all_display_data = all_display_data[[
                    "icon_href", "item_name", "Price_Formatted", "auction_count", 
                    "item_id", "rarity_color", "rarity_name", "price_color"
                ]]

                gb = GridOptionsBuilder.from_dataframe(all_display_data)
                gb.configure_selection('single', use_checkbox=False)

                # Hide unnecessary columns
                for col in ["item_id", "rarity_color", "rarity_name", "price_color"]:
                    gb.configure_column(col, hide=True)

                # Configure visible columns
                gb.configure_column("icon_href", header_name="", width=56, pinned="left")
                gb.configure_column("item_name", header_name="Item name", width=280)
                gb.configure_column("Price_Formatted", header_name="Price", width=120)
                gb.configure_column("auction_count", header_name="Posted", width=120, type=["numericColumn"])

                # Pagination config
                gb.configure_pagination(
                    paginationAutoPageSize=False,
                    paginationPageSize=20
                )

                # Icon renderer
                icon_renderer = JsCode("""
                class IconCellRenderer {
                    init(params) {
                        this.eGui = document.createElement('div');
                        this.eGui.style.display = 'flex';
                        this.eGui.style.justifyContent = 'center';
                        this.eGui.style.alignItems = 'center';
                        this.eGui.style.height = '100%';
                        
                        const img = document.createElement('img');
                        img.src = params.value || 'https://wow.zamimg.com/images/wow/icons/large/inv_misc_questionmark.jpg';
                        img.style.height = '32px';
                        img.style.width = '32px';
                        img.style.objectFit = 'contain';
                        img.style.borderRadius = '4px';
                        
                        img.onerror = function() {
                            this.src = 'https://wow.zamimg.com/images/wow/icons/large/inv_misc_questionmark.jpg';
                        };
                        
                        this.eGui.appendChild(img);
                    }
                    
                    getGui() {
                        return this.eGui;
                    }
                }
                """)

                price_style = JsCode("""
                    function(params) {
                        return {color: params.data.price_color || "#FFD700", fontWeight: "bold"};
                    }
                """)

                # Build rarity text color style and assign it dynamically
                rarity_name_style = JsCode("""
                    function(params) {
                        return {color: params.data.rarity_color || "#fff", fontWeight: "bold"};
                    }
                """)

                grid_options = gb.build()
                grid_options["pagination"] = True

                # Apply custom renders
                for col in grid_options["columnDefs"]:
                    if col["field"] == "icon_href":
                        col["cellRenderer"] = icon_renderer
                        col["sortable"] = False
                    elif col["field"] == "item_name":
                        col["cellStyle"] = rarity_name_style
                    elif col["field"] == "Price_Formatted":
                        col["cellStyle"] = price_style

                # The grid
                grid_response = AgGrid(
                    all_display_data,
                    gridOptions=grid_options,
                    theme="alpine",
                    allow_unsafe_jscode=True,
                    update_mode="SELECTION_CHANGED",
                    height=700,
                    # Custom CSS for grid & toolbar
                    custom_css={
                        "#gridToolBar": {"padding-bottom": "0px !important"},
                        ".ag-row": {"cursor": "pointer"},
                        ".ag-row:hover": {"background-color": "rgba(66, 165, 245, 0.1) !important"},
                        ".ag-header-cell": {"font-size": "12px"},
                        ".ag-paging-panel": {
                            "justify-content": "center !important",
                            "padding": "10px !important",
                        }
                    }
                )
                
                # Get selected rows from AgGrid
                selected_rows = grid_response['selected_rows']
            else:
                st.info("No auctions found with the current filters.")
                selected_rows = []

    # ---------- Auction details column ----------
    with col_auction_details:
        with st.container(border=True):
            st.subheader("Auction listings")
            if selected_rows:
                item_id = selected_rows[0]["item_id"]
                auction_listings_query = f"""
                    SELECT
                        auction_id,
                        MIN(buyout) AS "Price",
                        MIN(realm_name) AS "Realm name",
                        MIN(time_left) AS "Time left"
                    FROM refined.mart_market
                    WHERE item_id = {item_id}
                    GROUP BY auction_id
                    ORDER BY "Price" ASC
                """
                auctions_for_item = fetch_data_from_db(auction_listings_query)
                
                if not auctions_for_item.empty:
                    # Format the data for better display
                    auctions_display = format_auction_listings(auctions_for_item)
                    
                    # Prepare data for AgGrid
                    auction_grid_data = auctions_display[[
                        "Price_Formatted", "Realm name", "Time_Left_Formatted", 
                        "auction_id", "Price", "price_color", "time_color"  # Include color data
                    ]].copy()
                    
                    # Configure AgGrid for auction listings
                    gb_auctions = GridOptionsBuilder.from_dataframe(auction_grid_data)
                    gb_auctions.configure_selection('single', use_checkbox=False)
                    
                    # Hide reference columns
                    for col in ["auction_id", "Price", "price_color", "time_color"]:
                        gb_auctions.configure_column(col, hide=True)
                    
                    # Configure visible columns
                    gb_auctions.configure_column("Price_Formatted", header_name="Price", width=140, pinned="left")
                    gb_auctions.configure_column("Realm name", header_name="Realm", width=130)
                    gb_auctions.configure_column("Time_Left_Formatted", header_name="Time Left", width=150)
                    
                    # Disable pagination for auction listings (usually not many)
                    gb_auctions.configure_pagination(enabled=False)
                    
                    # FIXED: Use cellStyle like your working item names
                    price_style = JsCode("""
                        function(params) {
                            return {color: params.data.price_color || "#FFD700", fontWeight: "bold"};
                        }
                    """)
                    
                    time_style = JsCode("""
                        function(params) {
                            return {color: params.data.time_color || "#ffffff", fontWeight: "bold"};
                        }
                    """)
                    
                    auction_grid_options = gb_auctions.build()
                    
                    # Apply custom styles (using cellStyle, not cellRenderer)
                    for col in auction_grid_options["columnDefs"]:
                        if col["field"] == "Price_Formatted":
                            col["cellStyle"] = price_style
                        elif col["field"] == "Time_Left_Formatted":
                            col["cellStyle"] = time_style
                    
                    # Create the auction listings grid
                    auction_grid_response = AgGrid(
                        auction_grid_data,
                        gridOptions=auction_grid_options,
                        theme="alpine",
                        allow_unsafe_jscode=True,
                        update_mode="SELECTION_CHANGED",
                        height=700,
                        custom_css={
                            "#gridToolBar": {"padding-bottom": "0px !important"},
                            ".ag-row": {"cursor": "pointer"},
                            ".ag-row:hover": {"background-color": "rgba(255, 215, 0, 0.1) !important"},  # Gold hover
                            ".ag-header-cell": {"font-size": "12px"}
                        }
                    )
                    
                    # Show selected auction details if needed
                    selected_auction = auction_grid_response['selected_rows']
                    if selected_auction:
                        auction = selected_auction[0]
                        st.info(f"🎯 Selected auction: {auction['Price_Formatted']} on {auction['Realm name']}")
                    
                else:
                    st.info("No auction listings found for this item.")
            else:
                st.info("Select an item to see its available listings.")

    # ---------- Item details column ----------
    with col_item_details:
        with st.container(border=True):
            st.subheader("Item details")
            # Handle if selected_rows is a list of dicts (AgGrid default)
            if isinstance(selected_rows, list) and len(selected_rows) > 0:
                selected_id = selected_rows[0].get("item_id")
            elif hasattr(selected_rows, "iloc") and len(selected_rows) > 0:
                selected_id = selected_rows.iloc[0].get("item_id")
            else:
                selected_id = None

            item = None

            # Query the database with the selected auction (if selected)
            if selected_id is not None:
                item_query = f"""
                    SELECT *
                    FROM refined.mart_items
                    WHERE id = {selected_id}
                    LIMIT 1
                """
                item_df = fetch_data_from_db(item_query)
                if not item_df.empty:
                    item = item_df.iloc[0].to_dict()

                # Mapping of item detail tables
                ITEM_DETAIL_TABLES = {
                    "Weapon": "refined.mart_weapon_details",
                    "Armor": "refined.mart_armor_details",
                    # ...
                }

                details_query = f"""
                    SELECT *
                    FROM refined.mart_weapon_details
                    WHERE id = {selected_id}
                """
                details_df = fetch_data_from_db(details_query)

            # If an auctioned item is selected in the results dataframe
            if item:
                render_item_details(item, details_df)
            else:
                st.info("Select an item to see item details.")

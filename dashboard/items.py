import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
from .utils import fetch_data_from_db, get_sidebar_filters, build_items_query_conditions, render_active_filters
from .main_components import render_item_details
from .sidebar_components import free_text_search

# ---------- Items Page ----------
def items_page():
    st.title("Item Database")

    search_column, filters_column = st.columns([0.35, 0.65])

    with search_column:
        free_text_search()
    
    with filters_column:
        st.markdown("<div style='height: 1.75em'></div>", unsafe_allow_html=True)
        filters = get_sidebar_filters()
        render_active_filters(filters)

    # Build conditions + clause based on current filters
    conditions = build_items_query_conditions(filters)
    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    # Fetch ALL filtered data
    items_query = f"""
        SELECT
            id,
            item_name,
            item_level,
            required_level,
            rarity_name,
            icon_href
        FROM refined.mart_items
        {where_clause}
        ORDER BY item_name ASC, item_level DESC
    """
    items_data = fetch_data_from_db(query=items_query)

    # Placeholder icon url
    # items_data["icon"] = "https://wow.zamimg.com/images/wow/icons/large/inv_misc_questionmark.jpg"

    # Icon url
    items_data["icon_href"] = items_data["icon_href"].fillna("https://wow.zamimg.com/images/wow/icons/large/inv_misc_questionmark.jpg")
    columns_to_show = ["id", "icon_href", "item_name", "item_level", "required_level", "rarity_name"]
    items_data = items_data[columns_to_show]

    # Rarity color mapping
    rarity_colors = {
        "Poor": "#9d9d9d",
        "Common": "#ffffff",
        "Uncommon": "#1eff00",
        "Rare": "#0070dd",
        "Epic": "#a335ee",
        "Legendary": "#ff8000",
        "Artifact": "#e6cc80",
        "Heirloom": "#00ccff",
        "Heirloom Artifact": "#00ccff",
        "Wow Token": "#ffd700"
    }

    # Add rarity color to dataframe
    items_data["rarity_color"] = items_data["rarity_name"].map(rarity_colors)

    # Results column + details column (50/50%)
    col_results, col_details = st.columns([0.5, 0.5])

    # ---------- Results dataframe column ----------
    with col_results:
        if not items_data.empty:
            gb = GridOptionsBuilder.from_dataframe(items_data)
            gb.configure_selection('single', use_checkbox=False)
            gb.configure_column('id', hide=True) # Hide item id
            gb.configure_column('rarity_color', hide=True) # Hide rarity_color hex code
            # Change column labels
            # gb.configure_column("id", header_name="ID", width=100) # To be removed later
            gb.configure_column("item_name", header_name="Item name", width=280)
            gb.configure_column("item_level", header_name="iLvl", width=100)
            gb.configure_column("required_level", header_name="Req. lvl", width=100)
            gb.configure_column("rarity_name", header_name="Rarity", width=120)
            gb.configure_column(
                "icon_href",
                header_name="Image",
                cellRenderer="""
                    function(params) {
                        return `<img src="${params.value}" style="height:56px;width:56px;object-fit:contain;" />`
                    }
                """,
                width=60,
                pinned="left"
            )

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

            grid_response = AgGrid(
                items_data,
                gridOptions=grid_options,
                theme="alpine",
                allow_unsafe_jscode=True,
                update_mode="SELECTION_CHANGED",
                custom_css={
                    "#gridToolBar": {"padding-bottom": "0px !important"},
                    ".ag-paging-panel": {"justify-content": "flex-start !important"},
                }
            )
            selected_rows = grid_response['selected_rows']
        else:
            st.info("No items found with the current filters.")
            selected_rows = []

    # ---------- Item details column ----------
    with col_details:
        with st.container(border=True):
            st.subheader("Item details")
            # Handle if selected_rows is a list of dicts (AgGrid default)
            if isinstance(selected_rows, list) and len(selected_rows) > 0:
                selected_id = selected_rows[0].get("id")
            elif hasattr(selected_rows, "iloc") and len(selected_rows) > 0:
                selected_id = selected_rows.iloc[0].get("id")
            else:
                selected_id = None

            item = None
            
            # Query the database with the selected item (if selected)
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

            # If an item is selected in the results dataframe
            if item:
                render_item_details(item, details_df)
            else:
                st.info("Select an item in the results to see its details.")

"""
app_ui.py — Streamlit Dashboard for Project Pipeline API
Run: streamlit run app_ui.py

Adapted from AX-Scraper dashboard (app_ui.py).
Covers: scraping, product view, translations, variants, star ratings,
        bulk operations, export, and database stats.
"""

import streamlit as st
import requests
import json
import pandas as pd
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Pipeline Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

API_BASE = st.sidebar.text_input("API Base URL", value="http://localhost:8686")

# ── Custom CSS ────────────────────────────────────────────────────────────────

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Syne:wght@400;600;800&display=swap');

    html, body, [class*="css"] {
        font-family: 'Syne', sans-serif;
    }
    .stApp {
        background: #0d0d0d;
        color: #e8e8e8;
    }
    .block-container {
        padding-top: 2rem;
        max-width: 1400px;
    }
    h1, h2, h3 {
        font-family: 'Syne', sans-serif !important;
        font-weight: 800 !important;
        letter-spacing: -0.03em;
    }
    .metric-card {
        background: #1a1a1a;
        border: 1px solid #2a2a2a;
        border-radius: 12px;
        padding: 1.2rem 1.5rem;
        margin-bottom: 1rem;
    }
    .metric-card .label {
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: #888;
        margin-bottom: 0.3rem;
    }
    .metric-card .value {
        font-family: 'JetBrains Mono', monospace;
        font-size: 1.8rem;
        font-weight: 600;
        color: #f0f0f0;
    }
    .tag-success {
        background: #0f3d1e;
        color: #4ade80;
        padding: 2px 10px;
        border-radius: 20px;
        font-size: 0.75rem;
        font-family: 'JetBrains Mono', monospace;
        font-weight: 600;
    }
    .tag-fail {
        background: #3d0f0f;
        color: #f87171;
        padding: 2px 10px;
        border-radius: 20px;
        font-size: 0.75rem;
        font-family: 'JetBrains Mono', monospace;
        font-weight: 600;
    }
    .stButton > button {
        background: #ff4d00 !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        font-family: 'Syne', sans-serif !important;
        font-weight: 600 !important;
        letter-spacing: 0.04em !important;
        padding: 0.5rem 1.5rem !important;
        transition: all 0.2s !important;
    }
    .stButton > button:hover {
        background: #ff6a2a !important;
        transform: translateY(-1px) !important;
    }
    .stTextInput > div > div > input,
    .stTextArea > div > div > textarea,
    .stNumberInput > div > div > input {
        background: #1a1a1a !important;
        border: 1px solid #2a2a2a !important;
        color: #e8e8e8 !important;
        border-radius: 8px !important;
        font-family: 'JetBrains Mono', monospace !important;
    }
    .stSelectbox > div > div {
        background: #1a1a1a !important;
        border: 1px solid #2a2a2a !important;
        color: #e8e8e8 !important;
        border-radius: 8px !important;
    }
    .stDataFrame, .stTable {
        background: #1a1a1a !important;
    }
    .stExpander {
        background: #1a1a1a !important;
        border: 1px solid #2a2a2a !important;
        border-radius: 10px !important;
    }
    .stSidebar {
        background: #111111 !important;
        border-right: 1px solid #1e1e1e !important;
    }
    .stTabs [data-baseweb="tab-list"] {
        background: #1a1a1a;
        border-radius: 10px;
        padding: 4px;
        gap: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        background: transparent !important;
        color: #888 !important;
        border-radius: 8px !important;
        font-family: 'Syne', sans-serif !important;
        font-weight: 600 !important;
    }
    .stTabs [aria-selected="true"] {
        background: #ff4d00 !important;
        color: white !important;
    }
    .json-box {
        background: #111;
        border: 1px solid #222;
        border-radius: 10px;
        padding: 1rem;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.8rem;
        color: #a8d8a8;
        overflow-x: auto;
        max-height: 400px;
        overflow-y: auto;
    }
    .section-divider {
        border: none;
        border-top: 1px solid #1e1e1e;
        margin: 1.5rem 0;
    }
    div[data-testid="stSidebarNav"] { display: none; }
</style>
""", unsafe_allow_html=True)


# ── Helpers ───────────────────────────────────────────────────────────────────

def api(method: str, path: str, **kwargs):
    url = f"{API_BASE}{path}"
    try:
        resp = getattr(requests, method)(url, timeout=300, **kwargs)
        return resp.status_code, resp.json()
    except requests.exceptions.ConnectionError:
        return 0, {"error": f"Cannot connect to {API_BASE}"}
    except Exception as e:
        return 0, {"error": str(e)}


def show_json(data):
    st.markdown(
        f'<div class="json-box">{json.dumps(data, indent=2, ensure_ascii=False)}</div>',
        unsafe_allow_html=True,
    )


# ── Sidebar nav ───────────────────────────────────────────────────────────────

st.sidebar.markdown("## 🛒 Pipeline")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigation",
    [
        "🏠 Overview",
        "🔍 Scrape Product",
        "📦 Products",
        "⭐ Star Ratings",
        "🧬 Variants",
        "🌍 Translations",
        "🏪 Manufacturers",
        "📤 Export",
    ],
    label_visibility="collapsed",
)

st.sidebar.markdown("---")
st.sidebar.markdown(
    "<div style='font-size:0.7rem;color:#555;font-family:JetBrains Mono,monospace;'>Pipeline v3.5</div>",
    unsafe_allow_html=True,
)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════

if page == "🏠 Overview":
    st.markdown("# Pipeline Dashboard")
    st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)

    code, stats = api("get", "/stats")

    if code == 200 and isinstance(stats, dict):
        cols = st.columns(4)
        display = [
            ("scraped_products",  "Products Scraped"),
            ("translation",       "Translations"),
            ("varient",           "Variants"),
            ("product_details",   "Star Ratings"),
        ]
        for i, (key, label) in enumerate(display):
            with cols[i]:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="label">{label}</div>
                    <div class="value">{stats.get(key, 0)}</div>
                </div>""", unsafe_allow_html=True)

        st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)

        cols2 = st.columns(4)
        display2 = [
            ("enhanced_content",       "Enhanced"),
            ("category_assignments",   "Categorized"),
            ("mapped_products",        "Mapped"),
            ("template_outputs",       "Exported"),
        ]
        for i, (key, label) in enumerate(display2):
            with cols2[i]:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="label">{label}</div>
                    <div class="value">{stats.get(key, 0)}</div>
                </div>""", unsafe_allow_html=True)
    else:
        st.error(f"Cannot reach API at {API_BASE} (code={code})")
        if isinstance(stats, dict) and stats.get("error"):
            st.caption(stats["error"])

    st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)
    st.markdown("### Recent Products")
    _, products = api("get", "/scraped-products?limit=20")
    if isinstance(products, list) and products:
        df = pd.DataFrame([
            {
                "product_id":  p.get("product_id"),
                "title":       (p.get("title") or "")[:65],
                "price":       p.get("price") or "—",
                "scraped_at":  p.get("scraped_at") or "—",
                "exported_at": p.get("exported_at") or "—",
            }
            for p in products
        ])
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No products scraped yet.")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: SCRAPE PRODUCT
# ══════════════════════════════════════════════════════════════════════════════

elif page == "🔍 Scrape Product":
    st.markdown("# Scrape Product")
    st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["Single / Bulk URL", "Search URL"])

    with tab1:
        st.markdown("##### Paste AliExpress product URL(s)")
        url_input = st.text_area(
            "URLs (one per line or comma-separated)",
            placeholder="https://www.aliexpress.com/item/1005010388288135.html",
            height=100,
        )
        compliance = st.checkbox("Extract compliance info", value=True)

        if st.button("🚀 Scrape", key="scrape_btn"):
            raw = url_input.strip()
            if not raw:
                st.warning("Please enter at least one URL.")
            else:
                urls = [u.strip() for u in raw.replace(",", "\n").splitlines() if u.strip()]
                payload = {"urls": urls, "extract_compliance": compliance}
                with st.spinner(f"Scraping {len(urls)} URL(s)…"):
                    code, result = api("post", "/scrape-products", json=payload)

                if code == 200:
                    st.success(f"Done — {result.get('success', 0)} succeeded, {result.get('failed', 0)} failed")
                    for r in result.get("results", []):
                        icon = "✅" if r.get("success") else "❌"
                        with st.expander(f"{icon} {r.get('url', '')[:80]}"):
                            if r.get("success"):
                                st.markdown(f"**Product ID:** `{r.get('product_id')}`")
                                st.markdown(f"**Title:** {r.get('title', '—')}")
                            else:
                                st.error(r.get("error", "Unknown error"))
                else:
                    st.error(f"API error {code}")
                    show_json(result)

    with tab2:
        st.markdown("##### Scrape from a search results URL")
        search_url = st.text_input(
            "Search URL",
            placeholder="https://www.aliexpress.com/w/wholesale-bags.html?SearchText=bags"
        )
        max_pages = st.number_input("Max pages", min_value=1, max_value=50, value=3)
        delay = st.number_input("Delay between requests (s)", min_value=0.5, value=1.0)

        if st.button("🔍 Scrape Search", key="search_btn"):
            if not search_url.strip():
                st.warning("Enter a search URL.")
            else:
                with st.spinner("Scraping search results…"):
                    code, result = api("post", "/scrape-search", json={
                        "search_url": search_url,
                        "max_pages": max_pages,
                        "delay_between_requests": delay,
                    })
                if code == 200:
                    st.success(f"Found {result.get('total_found', 0)} products, saved {result.get('total_saved', 0)}")
                    show_json(result)
                else:
                    st.error(f"Error {code}")
                    show_json(result)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: PRODUCTS
# ══════════════════════════════════════════════════════════════════════════════

elif page == "📦 Products":
    st.markdown("# Products")
    st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["Browse", "Delete Product"])

    with tab1:
        limit = st.number_input("Limit", min_value=1, max_value=500, value=50)
        if st.button("Load Products", key="load_products"):
            code, data = api("get", f"/scraped-products?limit={limit}")
            if code == 200 and isinstance(data, list):
                df = pd.DataFrame([
                    {
                        "product_id":  p.get("product_id"),
                        "title":       (p.get("title") or "")[:70],
                        "price":       p.get("price") or "—",
                        "brand":       p.get("brand") or "—",
                        "store_name":  p.get("store_name") or "—",
                        "scraped_at":  p.get("scraped_at") or "—",
                    }
                    for p in data
                ])
                st.dataframe(df, use_container_width=True, hide_index=True)
            else:
                st.error(f"Error {code}")
                show_json(data)

    with tab2:
        st.markdown("Delete a product and all its related data (cascade).")
        st.warning("⚠️ This is irreversible. All related rows (seller info, compliance, translations, variants, etc.) will be deleted.")
        del_id = st.number_input("Product ID to delete", min_value=1, key="del_product_id")
        if st.button("🗑️ Delete Product", key="delete_product_btn"):
            code, result = api("delete", f"/products/fetched/{del_id}")
            if code == 200:
                st.success(result.get("message", "Deleted"))
            elif code == 404:
                st.error("Product not found")
            else:
                st.error(f"Error {code}")
                show_json(result)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: STAR RATINGS
# ══════════════════════════════════════════════════════════════════════════════

elif page == "⭐ Star Ratings":
    st.markdown("# Star Ratings & Delivery Dates")
    st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)
    st.markdown("Scrape star ratings, delivery dates, price, quantity, and ship country per AliExpress product ID.")

    tab1, tab2 = st.tabs(["Single Product", "Bulk Scrape"])

    with tab1:
        pid = st.text_input("AliExpress Product ID", placeholder="1005010388288135")
        if st.button("⭐ Scrape Details", key="scrape_details_btn"):
            if not pid.strip():
                st.warning("Enter a product ID.")
            else:
                with st.spinner("Scraping via Tor proxy…"):
                    code, result = api("post", "/product-details", json={"product_ids": [pid.strip()]})
                if code == 200:
                    results = result.get("results", [])
                    if results:
                        r = results[0]
                        st.success("Done")
                        cols = st.columns(4)
                        cols[0].metric("⭐ Rating",     r.get("rating") or "—")
                        cols[1].metric("📦 Delivery",   r.get("delivery") or "—")
                        cols[2].metric("💰 Price",      r.get("price") or "—")
                        cols[3].metric("🌍 Ship From",  r.get("ship_country") or "—")
                        if r.get("errors"):
                            st.warning(f"Missing fields: {', '.join(r['errors'])}")
                    else:
                        st.warning("No results returned.")
                else:
                    st.error(f"Error {code}")
                    show_json(result)

    with tab2:
        st.markdown("##### Paste product IDs (one per line)")
        ids_input = st.text_area("Product IDs", height=150, placeholder="1005010388288135\n1005006395261235")
        if st.button("⭐ Bulk Scrape", key="bulk_details_btn"):
            ids = [i.strip() for i in ids_input.strip().splitlines() if i.strip()]
            if not ids:
                st.warning("Enter at least one product ID.")
            else:
                with st.spinner(f"Scraping {len(ids)} products…"):
                    code, result = api("post", "/product-details", json={"product_ids": ids})
                if code == 200:
                    results = result.get("results", [])
                    st.success(f"Done — {len(results)} scraped")
                    if results:
                        df = pd.DataFrame([
                            {
                                "id":          r.get("id"),
                                "rating":      r.get("rating") or "—",
                                "delivery":    r.get("delivery") or "—",
                                "price":       r.get("price") or "—",
                                "quantity":    r.get("quantity") or "—",
                                "ship_from":   r.get("ship_country") or "—",
                                "errors":      ", ".join(r.get("errors", [])) or "none",
                            }
                            for r in results
                        ])
                        st.dataframe(df, use_container_width=True, hide_index=True)
                else:
                    st.error(f"Error {code}")
                    show_json(result)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: VARIANTS
# ══════════════════════════════════════════════════════════════════════════════

elif page == "🧬 Variants":
    st.markdown("# Product Variants")
    st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs(["Scrape Variants", "Bulk Job", "View / Delete"])

    with tab1:
        pid = st.text_input("AliExpress Product ID", placeholder="1005012117886583", key="var_pid")
        force = st.checkbox("Force re-scrape", key="var_force")
        if st.button("🧬 Scrape Variants", key="var_scrape_btn"):
            if not pid.strip():
                st.warning("Enter a product ID.")
            else:
                with st.spinner("Scraping variants…"):
                    code, result = api("post", "/scrape-variants", json={
                        "product_id": pid.strip(), "force_rescrape": force
                    })
                if code == 200:
                    source = result.get("source", "—")
                    st.success(f"Done — source: **{source}**")
                    variants = result.get("variants", {})
                    colors = variants.get("color", [])
                    sizes = variants.get("size", {})
                    if colors:
                        st.markdown(f"**Colors ({len(colors)}):**")
                        df_c = pd.DataFrame([{"name": c["name"], "sku": c.get("sku_col_id"), "selected": c.get("selected")} for c in colors])
                        st.dataframe(df_c, use_container_width=True, hide_index=True)
                    if sizes.get("plain_options"):
                        st.markdown(f"**Sizes ({len(sizes['plain_options'])}):** {', '.join(sizes['plain_options'][:20])}")
                    elif sizes.get("systems"):
                        for sys in sizes["systems"][:5]:
                            st.markdown(f"**{sys['country']}:** {', '.join(sys['options'][:10])}")
                else:
                    st.error(f"Error {code}")
                    show_json(result)

    with tab2:
        st.markdown("Run variant scraping as a **background job** for ALL stored products.")
        force_bulk = st.checkbox("Force re-scrape all", key="bulk_var_force")
        if st.button("🚀 Start Bulk Variant Job", key="bulk_var_btn"):
            with st.spinner("Launching…"):
                code, result = api("post", "/variants/bulk", json={"force_rescrape": force_bulk})
            if code == 202:
                st.success(f"Job started! ID: `{result.get('job_id')}`")
                st.markdown(f"- Total products: **{result.get('total_ids')}**")
                st.markdown(f"- Pending: **{result.get('pending_ids')}**")
                st.markdown(f"- Skipped: **{result.get('skipped')}**")
                st.code(result.get("job_id"), language=None)
            else:
                st.error(f"Error {code}")
                show_json(result)

        st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)
        st.markdown("##### Poll job status")
        job_id = st.text_input("Job ID", placeholder="paste job_id here", key="var_job_id")
        if st.button("📊 Check Status", key="var_job_status"):
            if job_id:
                code, result = api("get", f"/variants/bulk/{job_id}")
                if code == 200:
                    pct = result.get("progress_pct", 0)
                    st.progress(pct / 100)
                    cols = st.columns(4)
                    cols[0].metric("Status",    result.get("status", "—").upper())
                    cols[1].metric("Completed", result.get("completed", 0))
                    cols[2].metric("Failed",    result.get("failed", 0))
                    cols[3].metric("Progress",  f"{pct}%")
                else:
                    st.error(f"Error {code}")

    with tab3:
        pid_view = st.text_input("Product ID", placeholder="1005012117886583", key="var_view_pid")
        col1, col2 = st.columns(2)

        if col1.button("👁️ View Variants", key="view_var_btn"):
            if pid_view:
                code, result = api("get", f"/db/variants/{pid_view}")
                if code == 200:
                    show_json(result)
                else:
                    st.error(f"Error {code}")
                    show_json(result)

        if col2.button("🗑️ Delete Variants", key="del_var_btn"):
            if pid_view:
                code, result = api("delete", f"/db/variants/{pid_view}")
                if code == 200:
                    st.success(f"Deleted {result.get('rows_deleted', 0)} rows")
                else:
                    st.error(f"Error {code}")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: TRANSLATIONS
# ══════════════════════════════════════════════════════════════════════════════

elif page == "🌍 Translations":
    st.markdown("# Translations")
    st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)

    LANG_FLAGS = {
        "Romanian":   "🇷🇴",
        "German":     "🇩🇪",
        "Portuguese": "🇵🇹",
        "Finnish":    "🇫🇮",
        "French":     "🇫🇷",
    }

    tab1, tab2 = st.tabs(["Translate Product", "View Translations"])

    with tab1:
        st.markdown("Translates title, description, and specification into 5 languages: Romanian, German, Portuguese, **Finnish**, French.")
        aliexpress_id = st.text_input("AliExpress Product ID", placeholder="1005010388288135", key="trans_pid")
        if st.button("🌍 Translate", key="translate_btn"):
            if not aliexpress_id.strip():
                st.warning("Enter a product ID.")
            else:
                with st.spinner("Translating via LLM (~30s)…"):
                    code, result = api("post", f"/translate-product/{aliexpress_id.strip()}")
                if code == 200:
                    langs_done = result.get("languages_translated", [])
                    st.success(f"Translated into: {', '.join(langs_done)}")
                    for lang, fields in result.get("translations", {}).items():
                        flag = LANG_FLAGS.get(lang, "🌐")
                        with st.expander(f"{flag} {lang}"):
                            st.markdown(f"**Title:** {fields.get('title', '—')}")
                            st.markdown(f"**Description:** {(fields.get('description') or '—')[:300]}")
                            st.markdown(f"**Specification:** {(fields.get('specification') or '—')[:300]}")
                else:
                    st.error(f"Error {code}")
                    show_json(result)

    with tab2:
        st.markdown("##### Lookup translations for a product")
        aliexpress_id_v = st.text_input("AliExpress Product ID", placeholder="1005010388288135", key="view_trans_pid")
        if st.button("Load Translations", key="load_trans_btn"):
            if aliexpress_id_v.strip():
                code, data = api("get", f"/translations/{aliexpress_id_v.strip()}")
                if code == 200 and data:
                    for row in data:
                        lang = row.get("language", "—")
                        flag = LANG_FLAGS.get(lang, "🌐")
                        with st.expander(f"{flag} {lang}"):
                            st.markdown(f"**Title:** {row.get('title') or '—'}")
                            st.markdown(f"**Description:** {(row.get('description') or '—')[:400]}")
                            st.markdown(f"**Specification:** {(row.get('specification') or '—')[:400]}")
                            st.caption(f"Translated at: {row.get('translated_at', '—')}")
                elif code == 404:
                    st.info("No translations found. Run Translate first.")
                else:
                    st.error(f"Error {code}")
                    show_json(data)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: MANUFACTURERS
# ══════════════════════════════════════════════════════════════════════════════

elif page == "🏪 Manufacturers":
    st.markdown("# Manufacturer / Compliance Info")
    st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)

    limit = st.number_input("Limit", min_value=1, max_value=500, value=50)
    if st.button("Load"):
        code, data = api("get", f"/manufacturer?limit={limit}")
        if code == 200 and data:
            df = pd.DataFrame(data)
            st.dataframe(df, use_container_width=True, hide_index=True)
        elif code == 200:
            st.info("No manufacturer data found.")
        else:
            st.error(f"Error {code}")
            show_json(data)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: EXPORT
# ══════════════════════════════════════════════════════════════════════════════

elif page == "📤 Export":
    st.markdown("# Export Templates")
    st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)
    st.markdown("Export categorized products to per-category `.xlsm` files.")

    mode = st.radio(
        "Export mode",
        ["Full rebuild (all products)", "Incremental (only new/unexported)"],
        horizontal=True,
    )
    only_new = mode.startswith("Incremental")

    if st.button("📤 Run Export"):
        with st.spinner("Exporting…"):
            code, result = api("post", f"/export-templates?only_new={str(only_new).lower()}")
        if code == 200:
            st.success(
                f"Exported **{result.get('total_products', 0)}** products "
                f"across **{result.get('total_categories', 0)}** categories"
            )
            for f in result.get("files", []):
                with st.expander(f"📁 {f.get('category_name', '—')} ({f.get('product_count', 0)} products)"):
                    st.markdown(f"**Category ID:** `{f.get('category_id')}`")
                    st.markdown(f"**File:** `{f.get('file')}`")
        else:
            st.error(f"Error {code}")
            show_json(result)

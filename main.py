import streamlit as st
import pandas as pd
import requests
import random
import time
from datetime import date
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import re


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; rv:95.0) Gecko/20100101 Firefox/95.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36"
]

# Page configuration
st.set_page_config(
    page_title="BrandCentral Amazon Scraper | Enterprise",
    layout="wide",
    page_icon="🟠"
)

# Sidebar content
with st.sidebar:
    st.image("https://brandcentral.in/wp-content/uploads/2024/09/logo.png", width=180)
    # st.markdown("<h2 style='color:#ff9900;font-weight:bold;'>BrandCentral Suite</h2>", unsafe_allow_html=True)
    st.markdown("---")
    st.write("Empowering 500+ brands with real-time analytics, AI ops & automation")
    st.markdown("---")
    st.subheader("🔗 Quick Links")
    st.markdown("[💻 BrandCentral Website](https://brandcentral.in)")
    st.markdown("[📊 Dashboard Portal](https://amazon-scrapper.streamlit.app/)")
    st.markdown("[📧 Email Support](mailto:info@brandcentral.in)")
    st.markdown("---")
    st.subheader("📦 Enterprise Modules")
    st.write("""
    - Market Data & Trends  
    - Product Analytics  
    - Inventory Automation  
    - AI Marketing Tools  
    - Revenue Growth Solutions
    """)
    st.markdown("---")
    st.caption("© 2025 BrandCentral | Made in 🇮🇳 for Indian Enterprises")

# Main header
st.markdown("<h1 style='color:#ff9900;font-weight:700;'>BrandCentral Amazon.in Enterprise Scraper</h1>", unsafe_allow_html=True)
st.markdown("""
    <i>Enterprise-grade Amazon.in product scraper with multithreading, proxy rotation, and advanced price extraction.</i><br>
    <small>Powered by BrandCentral - Empowering Indian eCommerce growth.</small>
""", unsafe_allow_html=True)

# Input controls
today = st.date_input("Extraction Date", date.today())
output_filename = f"brandcentral_amazon_extract_{today.strftime('%Y%m%d')}.xlsx"
input_cols = st.columns([3, 1])
input_method = input_cols[0].radio("Input ASINs via:", ["Manual Input", "File Upload"], horizontal=True)
worker_count = input_cols[1].number_input("Concurrent Threads", 1, 10, 4)
timeout_col, retries_col, proxy_col = st.columns(3)
timeout = timeout_col.number_input("Timeout (sec)", 1, 30, 10)
retries = retries_col.slider("Retries per ASIN", 1, 5, 2)
use_proxy = proxy_col.checkbox("Use Proxy Rotation", value=False)

# Load ASINs
asins = []
if input_method == "Manual Input":
    raw_asins = st.text_area("Enter ASINs or Amazon URLs (one per line):")
    if raw_asins:
        for line in raw_asins.strip().splitlines():
            line = line.strip()
            if line:
                if "amazon" in line and "/dp/" in line:
                    try:
                        asin = line.split('/dp/')[1].split('/')[0]
                        asins.append(asin)
                    except Exception:
                        asins.append(line)
                else:
                    asins.append(line)
elif input_method == "File Upload":
    uploaded_file = st.file_uploader("Upload ASIN List (.csv, .xlsx, .txt)", type=["csv", "xlsx", "txt"])
    if uploaded_file:
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
            asins = df.iloc[:, 0].dropna().astype(str).tolist()
        elif uploaded_file.name.endswith('.xlsx'):
            df = pd.read_excel(uploaded_file)
            asins = df.iloc[:, 0].dropna().astype(str).tolist()
        elif uploaded_file.name.endswith('.txt'):
            text = uploaded_file.read().decode('utf-8', errors='ignore')
            asins = [line.strip() for line in text.splitlines() if line.strip()]

st.write(f"Total ASINs ready: {len(asins)}")

def load_proxies(filename="proxies.txt"):
    try:
        with open(filename) as f:
            return [p.strip() for p in f if p.strip()]
    except:
        return []

def get_random_proxy(proxies):
    if proxies:
        proxy_address = random.choice(proxies)
        return {"http": f"http://{proxy_address}", "https": f"http://{proxy_address}"}
    return None

def extract_price(soup, html):
    price_span = soup.find("span", class_="a-price")
    if price_span:
        whole = price_span.find("span", class_="a-price-whole")
        fraction = price_span.find("span", class_="a-price-fraction")
        if whole and fraction:
            return f"{whole.text.strip()}.{fraction.text.strip()}"
    offscreen_price = soup.find("span", class_="a-offscreen")
    if offscreen_price:
        return offscreen_price.text.strip()
    for pid in ["priceblock_ourprice", "priceblock_dealprice", "priceblock_saleprice"]:
        ptag = soup.find("span", id=pid)
        if ptag:
            return ptag.text.strip()
    meta_price = soup.find("meta", itemprop="price")
    if meta_price and meta_price.get("content"):
        return meta_price["content"]
    match = re.search(r"₹[\d,]+\.?\d*", html)
    if match:
        return match.group()
    return "Not Available"

def extract_category(soup):
    nav = soup.find("ul", class_="a-unordered-list a-horizontal a-size-small")
    if nav:
        cats = [span.get_text(strip=True) for span in nav.find_all("span", class_="a-list-item")]
        if cats:
            return " > ".join(cats)
    return "Unknown"

def scrape_asin(asin, timeout, proxies, retries):
    url = f"https://www.amazon.in/dp/{asin}"
    last_error = ""
    start_time = time.time()
    attempt = 0
    while attempt < retries:
        try:
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept-Language": "en-IN,en-US;q=0.9"
            }
            proxy = get_random_proxy(proxies) if proxies else None
            response = requests.get(url, headers=headers, proxies=proxy, timeout=timeout)
            time.sleep(random.uniform(2, 4))  # politeness delay
            response.raise_for_status()
            if "captcha" in response.text.lower() or response.status_code == 503:
                last_error = "Captcha detected"
                attempt += 1
                continue

            soup = BeautifulSoup(response.text, "html.parser")

            avail_tag = soup.find("div", id="availability")
            avail_text = avail_tag.get_text(strip=True) if avail_tag else "Unknown"
            is_available = not ("currently unavailable" in avail_text.lower() or "out of stock" in avail_text.lower())

            price = extract_price(soup, response.text) if is_available else "Not Available"

            return {
                "ASIN": asin,
                "Title": soup.find("span", id="productTitle").get_text(strip=True) if soup.find("span", id="productTitle") else "Unknown",
                "Price": price,
                "Availability": avail_text,
                "Category": extract_category(soup),
                "Time Taken (s)": f"{time.time() - start_time:.2f}",
                "Status": "Success",
                "Error": "",
                "Retries": str(attempt)
            }
        except Exception as e:
            last_error = str(e)
            attempt += 1
            time.sleep(random.uniform(1, 3))
    return {
        "ASIN": asin, "Title": "", "Price": "", "Availability": "",
        "Category": "", "Time Taken (s)": "",
        "Status": "Failed", "Error": last_error, "Retries": str(retries)
    }

start_button = st.button("Start Scraping", disabled=not bool(asins))
clear_button = st.button("Clear")

if start_button:
    proxy_list = load_proxies() if use_proxy else []
    result_list = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    result_table = st.empty()

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {executor.submit(scrape_asin, asin, timeout, proxy_list, retries): asin for asin in asins}
        for idx, future in enumerate(as_completed(futures)):
            res = future.result()
            result_list.append(res)
            progress_bar.progress((idx + 1) / len(asins))
            status_text.info(f"Processed {idx + 1} of {len(asins)}: {res['ASIN']} Status: {res['Status']}")
            result_table.dataframe(pd.DataFrame(result_list), use_container_width=True, height=400)

    output_buffer = io.BytesIO()
    pd.DataFrame(result_list).to_excel(output_buffer, index=False)
    output_buffer.seek(0)
    st.download_button("Download Results Excel", data=output_buffer, file_name=output_filename)
    st.success("Scraping completed!")

if clear_button:
    st.experimental_rerun()

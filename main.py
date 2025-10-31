import streamlit as st
import pandas as pd
import requests
import random
from datetime import date
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup

# --------- BRANDING AND LAYOUT ---------
st.set_page_config(
    "BrandCentral Amazon Scraper | Enterprise",
    layout="wide",
    page_icon="🟠"
)
st.markdown(
    "<h1 style='color:#ff9900;font-weight:700;'>BrandCentral Amazon.in Enterprise Scraper</h1>",
    unsafe_allow_html=True)
st.write("""
<b>Enterprise-Grade Amazon.in Data Extraction Suite</b>  
For: <span style='color:#6366f1'>Enterprise, Marketplace, eCommerce, Analytics, DataOps</span>  
<i>Turbo-charged scrape engine | Multi-proxy | Robust pipeline | ⓒ BrandCentral</i>
""", unsafe_allow_html=True)

# --------- SIDEBAR: ENTERPRISE STYLE ---------
with st.sidebar:
    st.image(
        "https://brandcentral.in/wp-content/uploads/2022/07/brandcentral-logo.png",
        width=180
    )
    st.markdown("<h3 style='margin-top:-18px;'>BrandCentral | Enterprise Suite</h3>", unsafe_allow_html=True)
    st.write("Empowering 500+ brands with analytics, automation, AI ops and real-time e-commerce intelligence.")
    st.markdown("---")
    st.subheader("🔗 Quick Access")
    st.write("• [BrandCentral Website](https://brandcentral.in)")
    st.write("• [Dashboard Portal](https://datasee.streamlit.app/)")
    st.write("• [Email Support](mailto:info@brandcentral.in)")
    st.markdown("---")
    st.subheader("📦 Modules")
    st.write("• Market Data + Trends\n• Product Analytics\n• Inventory Automation\n• AI Marketing\n• Revenue Growth Tools")
    st.markdown("---")
    st.caption("Made in 🇮🇳 for Indian Enterprise · © 2025 BrandCentral")

# --------- CONFIGURATION ---------
dropbox_key = st.text_input("Dropbox Access Key (optional):", type="password")
today = st.date_input("Extraction Date", date.today())
output_filename = f"brandcentral_amazon_extract_{today.strftime('%Y%m%d')}.xlsx"
cols = st.columns([2, 2])
input_method = cols[0].radio("Input Method", ["Manual Input", "File Upload"], horizontal=True)
worker_count = cols[1].number_input("Concurrent Threads", 1, 16, 8)
timeout, retries, use_proxy = st.columns(3)
timeout = timeout.number_input("Timeout (sec)", 1, 30, 11)
retries = retries.slider("Retries", 1, 3, 2)
use_proxy = use_proxy.checkbox("Proxy Rotation", value=False)

st.info("Paste ASINs/URLs below, or upload a list. Supported: CSV/XLSX/TXT.")

# --------- FLEXIBLE INPUT ---------
asins = []
if input_method == "Manual Input":
    raw = st.text_area("ASINs / Amazon URLs (one per line):")
    if raw:
        for line in raw.strip().splitlines():
            line = line.strip()
            if not line:
                continue
            if "amazon" in line and "/dp/" in line:
                try:
                    asins.append(line.split("/dp/")[1].split("/")[0])
                except:
                    asins.append(line)
            else:
                asins.append(line)
elif input_method == "File Upload":
    file = st.file_uploader("Upload ASIN List", type=["csv", "xlsx", "txt"])
    if file:
        if file.name.endswith(".csv"):
            df = pd.read_csv(file)
            asins = df.iloc[:, 0].astype(str).tolist()
        elif file.name.endswith(".xlsx"):
            df = pd.read_excel(file)
            asins = df.iloc[:, 0].astype(str).tolist()
        elif file.name.endswith(".txt"):
            asins = file.read().decode(errors="ignore").splitlines()
        asins = [a.strip() for a in asins if a.strip()]

st.write(f"**ASINs loaded:** {len(asins)}")

# --------- PROXY HANDLING ---------
def load_proxies(filename="proxies.txt"):
    try:
        with open(filename) as f:
            return [i.strip() for i in f if i.strip()]
    except:
        return []

def get_proxy(proxies):
    if not proxies:
        return None
    p = random.choice(proxies)
    return {"http": f"http://{p}", "https": f"http://{p}"}

# --------- ADVANCED FIELD SCRAPERS ---------
def extract_price(soup):
    # 1. a-price-whole + a-price-fraction
    whole = soup.find("span", {"class": "a-price-whole"})
    fraction = soup.find("span", {"class": "a-price-fraction"})
    if whole and fraction:
        return f"{whole.get_text(strip=True)}.{fraction.get_text(strip=True)}"
    # 2. a-offscreen
    price_tag = soup.find("span", {"class": "a-offscreen"})
    if price_tag:
        return price_tag.get_text(strip=True)
    # 3. id-based price
    for pid in ["priceblock_ourprice", "priceblock_dealprice", "priceblock_saleprice"]:
        ptag = soup.find("span", {"id": pid})
        if ptag:
            return ptag.get_text(strip=True)
    # 4. Fallback: scan for ₹ in any span
    for span in soup.find_all("span"):
        txt = span.get_text(strip=True)
        if "₹" in txt:
            return txt
    return ""

def extract_category(soup):
    # nav breadcrumbs
    nav = soup.find("ul", {"class": "a-unordered-list a-horizontal a-size-small"})
    if nav:
        items = nav.find_all("span", {"class": "a-list-item"})
        cats = [i.get_text(strip=True) for i in items if i.get_text(strip=True)]
        if cats:
            return " > ".join(cats)
    # li breadcrumbs
    li_breadcrumbs = soup.find_all("li", {"class": "a-breadcrumb-item"})
    if li_breadcrumbs:
        return " > ".join([li.get_text(strip=True) for li in li_breadcrumbs if li])
    # tertiary category links
    cat = soup.find("a", {"class": "a-link-normal a-color-tertiary"})
    if cat:
        return cat.get_text(strip=True)
    return ""

# --------- MAIN SCRAPER ---------
def scrape_asin(asin, timeout, proxies=None, retries=2):
    from time import time as timer
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Accept-Language": "en-IN,en-US;q=0.9"}
    url = f"https://www.amazon.in/dp/{asin.strip()}"
    proxy = get_proxy(proxies) if proxies else None
    last_error = ""
    t0 = timer()
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, proxies=proxy, timeout=timeout)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            res = {"ASIN": asin}
            res["Status"] = "Success"
            res["Title"] = soup.find("span", id="productTitle").get_text(strip=True) if soup.find("span", id="productTitle") else ""
            res["Price"] = extract_price(soup)
            res["Original Price"] = ""
            res["Rating"] = soup.find("span", id="acrPopover")["title"] if soup.find("span", id="acrPopover") and soup.find("span", id="acrPopover").has_attr("title") else ""
            reviews_tag = soup.find("span", id="acrCustomerReviewText")
            res["Reviews"] = reviews_tag.get_text(strip=True) if reviews_tag else ""
            res["Brand"] = ""
            avail_tag = soup.find("div", id="availability")
            avail_text = avail_tag.get_text(strip=True) if avail_tag else ""
            res["Availability"] = "In Stock" if "in stock" in avail_text.lower() else "Available"
            res["Currently Unavailable Status"] = "Yes" if "currently unavailable" in avail_text.lower() else "No"
            res["Category"] = extract_category(soup)
            res["BSR"] = ""
            res["Sub BSR"] = ""
            res["GL Category"] = ""
            res["A Content"] = "Yes" if soup.find("div", id="aplus") else "No"
            res["Video"] = "Yes" if soup.find("div", {"class": "video-block"}) else "No"
            res["Coupon"] = "Yes" if soup.find("span", {"class": "coupon"}) else "Not Found"
            res["ASIN Match"] = "Same"
            img_tag = soup.find("img", id="landingImage")
            res["Image URL"] = img_tag["src"] if img_tag and img_tag.has_attr("src") else ""
            deal_tag = soup.find("span", string=lambda x: x and ("deal" in x.lower()))
            res["Deal"] = deal_tag.get_text(strip=True) if deal_tag else "Not Found"
            seller_tag = soup.find("a", id="bylineInfo")
            res["Seller"] = seller_tag.get_text(strip=True) if seller_tag else ""
            res["Time s"] = f"{timer()-t0:.2f}"
            res["Retries"] = str(attempt)
            res["Error"] = ""
            return res
        except Exception as e:
            last_error = str(e)
            proxy = get_proxy(proxies) if proxies else None
    return {
        "ASIN": asin, "Status": "Error", "Title": "", "Price": "",
        "Original Price": "", "Rating": "", "Reviews": "", "Brand": "",
        "Availability": "", "Currently Unavailable Status": "",
        "Category": "", "BSR": "", "Sub BSR": "", "GL Category": "", "A Content": "",
        "Video": "", "Coupon": "", "ASIN Match": "", "Image URL": "",
        "Deal": "", "Seller": "", "Time s": "", "Retries": str(retries),
        "Error": last_error
    }

# --------- DASHBOARD LOGIC ---------
start_btn = st.button("🚀 Launch Extraction", disabled=not bool(asins))
clear_btn = st.button("🗑️ Clear Results", key="clear")

if start_btn:
    proxy_list = load_proxies() if use_proxy else []
    results = []
    progress = st.progress(0)
    status = st.empty()
    with ThreadPoolExecutor(max_workers=int(worker_count)) as executor:
        tasks = {executor.submit(scrape_asin, asin, timeout, proxy_list, retries): asin for asin in asins}
        for i, future in enumerate(as_completed(tasks)):
            res = future.result()
            results.append(res)
            progress.progress((i+1)/len(asins))
            status.info(f"Scrapped ({i+1}/{len(asins)}): {res.get('ASIN')}")
    df = pd.DataFrame(results)
    st.dataframe(df)
    output = io.BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)
    st.download_button("Download Excel", data=output, file_name=output_filename)
    st.success(f"DONE! {len(results)} ASINs processed. File ready.")
elif clear_btn:
    st.experimental_rerun()

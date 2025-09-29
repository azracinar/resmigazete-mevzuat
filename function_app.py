import os
import re
import json
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
from urllib.parse import urljoin
import azure.functions as func
from azure.storage.blob import BlobServiceClient

# ---- config ----
CONTAINER_NAME = "resmigazete"
FILE_PREFIX = "resmigazete_"
FILE_SUFFIX = ".json"

# --- filter out the footer of the page ----
BLACKLIST = [
    "Genel Arama",
    "Arşiv",
    "Mükerrer Arşivi",
    "Resmî Gazete Tarihçesi",
    "Resmî Gazete Mevzuatı",
    "Mevzuat Bilgi Sistemi",
    "0 (312) 525 3427",
    "Bize Ulaşın",
    "Haritada göster"
]

# --- to indicate whether Yeni Yonetmelik or Yonetmelik Degisikligi ---
DEGISIKLIK_KEYWORDS = [
    "değişikliği",
    "değiştirilmesine",
    "değişiklik yapılmasına",
    "değiştirilmiş"
]

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# ---- helpers ----
def _temizle(metin: str) -> str:
    return re.sub(r"\s+", " ", (metin or "")).strip()

def _classify(baslik: str) -> str:
    t = baslik.casefold()
    if "yönetmelik" in t or "yonetmelik" in t: return "Yönetmelik"
    if "tebliğ" in t or "teblig" in t:        return "Tebliğ"
    if "karar" in t:                           return "Karar"
    if "ilan" in t:                            return "İlan"
    return "Diğer"

def classify_yonetmelik(title: str) -> str:
    title_lower = title.lower()
    for kw in DEGISIKLIK_KEYWORDS:
        if kw in title_lower:
            return "Değişiklik"
    return "Yeni"

# --- Retry-enabled HTTP session ---
def _http_session():
    """Create a requests session with retries and backoff."""
    session = requests.Session()
    retries = Retry(
        total=5,                 # retry up to 5 times
        backoff_factor=2,        # wait 2s, 4s, 8s, 16s...
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def extract_text_from_page(link: str) -> str:
    """Extract full text from a given Resmî Gazete detail page."""
    s = _http_session()
    try:
        r = s.get(link, timeout=20)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        return f"Hata: Sayfa alınamadı ({e})"

    soup = BeautifulSoup(r.text, "html.parser")
    content = soup.get_text(separator="\n", strip=True)
    return content[:2000]  # prevent storing overly large text

def scrape_resmigazete() -> list[dict]:
    """Scrape today's Resmî Gazete and return rows as list of dicts."""
    url = "https://www.resmigazete.gov.tr/"
    s = _http_session()

    try:
        r = s.get(url, timeout=20)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        return [{"error": f"Failed to fetch Resmî Gazete: {e}"}]
        
    soup = BeautifulSoup(r.text, "html.parser")

    today_str = datetime.utcnow().strftime("%d.%m.%Y")
    rows = []

    for tag in soup.find_all(["a", "td"]):
        baslik = tag.get_text(strip=True)
        if len(baslik) < 5:
            continue
        href = tag.get("href")
        link = urljoin(url, href) if href else None
        if not link or "tarihli ve" in baslik.lower():
            continue
        if baslik in BLACKLIST:
            continue

        kategori = _classify(baslik)

        row = {
            "Tarih": today_str,
            "Kategori": kategori,
            "Başlık": baslik,
        }

        if kategori == "Yönetmelik":
            yonetmelik_turu = classify_yonetmelik(baslik)
            row["Yönetmelik Türü"] = yonetmelik_turu

            if yonetmelik_turu == "Yeni":
                row["Link"] = link
            else:  # Degisiklik
                row["Tam Metin"] = extract_text_from_page(link)
        else:
            row["Link"] = link

        rows.append(row)

    return rows

# ---- HTTP endpoint ----
@app.route(route="scrape", methods=["GET"])
def scrape(req: func.HttpRequest) -> func.HttpResponse:
    """Always return the latest JSON file from Blob Storage."""
    try:
        conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        blob_service = BlobServiceClient.from_connection_string(conn_str)
        container = blob_service.get_container_client(CONTAINER_NAME)

        # Find the latest blob
        blobs = list(container.list_blobs())
        if not blobs:
            return func.HttpResponse(
                json.dumps({"error": "No JSON files found in container."}, ensure_ascii=False, indent=2),
                status_code=404,
                mimetype="application/json",
            )

        latest_blob = max(blobs, key=lambda b: b.last_modified)
        blob_data = container.get_blob_client(latest_blob.name).download_blob().readall()

        return func.HttpResponse(
            body=blob_data,
            status_code=200,
            mimetype="application/json",
            headers={"Cache-Control": "no-store"},
        )

    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}, ensure_ascii=False, indent=2),
            status_code=500,
            mimetype="application/json",
        )

# ---- Script mode (for GitHub Actions) ----
if __name__ == "__main__":
    data = scrape_resmigazete()
    today_str = datetime.utcnow().strftime("%d.%m.%Y")
    filename = f"{FILE_PREFIX}{today_str}{FILE_SUFFIX}"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"✅ Saved {filename}")







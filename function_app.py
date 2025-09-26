import os
import re
import json
import pandas as pd
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

def _http_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=1,
                    status_forcelist=[429, 500, 502, 503, 504],
                    allowed_methods=["GET"], raise_on_status=False)
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    })
    return s

def scrape_resmigazete() -> list[dict]:
    """Scrape today's Resmî Gazete and return rows as list of dicts."""
    url = "https://www.resmigazete.gov.tr/"
    s = _http_session()
    r = s.get(url, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    today_str = datetime.utcnow().strftime("%d.%m.%Y")
    rows = []
    for tag in soup.find_all(["a", "td"]):
        baslik = tag.get_text(strip=True)
        if len(baslik) < 5:
            continue
        href = tag.get("href")
        full_url = urljoin(url, href) if href else None
        if not full_url or "tarihli ve" in baslik.lower():
            continue

        kategori = _classify(baslik)
        rows.append({
            "Tarih": today_str,
            "Kategori": kategori,
            "Başlık": baslik,
            "Link": full_url,
        })
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
                json.dumps({"error": "No JSON files found in container."}),
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
            json.dumps({"error": str(e)}),
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





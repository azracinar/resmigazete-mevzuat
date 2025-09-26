import os
import re
from datetime import datetime
from urllib.parse import urljoin
from zoneinfo import ZoneInfo
import azure.functions as func
import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# ---- yardımcılar ----
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
    """Retry + User-Agent + (varsa) proxy içeren session."""
    s = requests.Session()

    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))

    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        )
    })

    # --- Proxy ayarı (PROXY_URL varsa devreye girer) ---
    proxy = os.getenv("PROXY_URL", "").strip()
    if proxy:
        s.proxies = {"http": proxy, "https": proxy}
        # Kurumsal proxy SSL'i kesiyorsa (MITM), CA sertifikanın yolunu ver:
        # cert_path = os.getenv("REQUESTS_CA_BUNDLE", "").strip()
        # if cert_path:
        #     s.verify = cert_path

    # --- No proxy (isteğe bağlı) ---
    # Örn. iç kaynaklar için bypass etmek istersen: NO_PROXY="localhost,127.0.0.1"
    no_proxy = os.getenv("NO_PROXY", "").strip()
    if no_proxy:
        os.environ["NO_PROXY"] = no_proxy  # requests bunu otomatik kullanır

    return s

# ---- HTTP endpoint ----
@app.route(route="scrape", methods=["GET"])
@app.route(route="scrape", methods=["GET"])
def scrape(req: func.HttpRequest) -> func.HttpResponse:
    """
    Always returns today's JSON (uploaded by GitHub Actions) from Azure Blob Storage.
    """
    try:
        conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]

        # Format today’s date as DD.MM.YYYY
        today_str = datetime.utcnow().strftime("%d.%m.%Y")
        target_name = f"{FILE_PREFIX}{today_str}{FILE_SUFFIX}"

        blob_service = BlobServiceClient.from_connection_string(conn_str)
        container    = blob_service.get_container_client(CONTAINER_NAME)

        blob = container.download_blob(target_name).readall()
        
        soup = BeautifulSoup(r.text, "html.parser")

        rows = []
        for tag in soup.find_all(["a", "td"]):
            baslik = tag.get_text(strip=True)
            if len(baslik) < 5:
                continue
            href = tag.get("href")
            full_url = urljoin(ana_url, href) if href else None
            if not full_url or "tarihli ve" in baslik.lower():
                continue

            kategori = _classify(baslik)

            # Yönetmeliklerde metni de çek
            if kategori == "Yönetmelik":
                try:
                    r2 = s.get(full_url, timeout=45)
                    if r2.ok:
                        inner = BeautifulSoup(r2.text, "html.parser")
                        metin = _temizle(inner.get_text(separator=" "))
                    else:
                        metin = full_url  # linki döndür
                except Exception as e:
                    metin = f"[HATA: {e}]"
            else:
                metin = full_url

            rows.append({
                "Tarih": tarih_str_xls,
                "Kategori": kategori,
                "Yönetmelik Türü": "",
                "Başlık": baslik,
                "HTML linki": full_url,
                "Değişiklik Kapsamı": metin
            })

        # JSON döndür
        df = pd.DataFrame(rows)
        return func.HttpResponse(
            body=blob,
            status_code=200,
            mimetype="application/json",
            headers={"Cache-Control": "no-store"}
        )

    except requests.exceptions.Timeout:
        return func.HttpResponse("[]", mimetype="application/json", status_code=200)
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500, mimetype="application/json"
        )

        )

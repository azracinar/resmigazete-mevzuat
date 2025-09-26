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
def scrape(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Always use today's date in Turkey local time
        today = datetime.now(ZoneInfo("Europe/Istanbul"))
        tarih_str     = today.strftime("%Y%m%d")
        tarih_str_xls = today.strftime("%d.%m.%Y")

        #eskiler = f"https://www.resmigazete.gov.tr/eskiler/{y}/{m}/{tarih_str}.htm"
        ana_url = f"https://www.resmigazete.gov.tr/default.aspx"

        s = _http_session()
        r = s.get(ana_url, timeout=100)
        # Gün sayfası yoksa boş liste döndür (ör. resmi tatil/güncel değil)
        if r.status_code == 404: 
            return r.status_code
        elif not (r.text or "").strip():
            return func.HttpResponse("Resmi tatil", mimetype="application/json", status_code=200)
        r.raise_for_status()

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
            df.to_json(orient="records", force_ascii=False),
            mimetype="application/json",
            status_code=200
        )

    except requests.exceptions.Timeout:
        return func.HttpResponse("[]", mimetype="application/json", status_code=200)
    except Exception as e:
        return func.HttpResponse(
            f"Error while preparing date: {str(e)}",
            status_code=500
        )
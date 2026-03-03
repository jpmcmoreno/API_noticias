"""
News Scraper API - FastAPI Service
Scraping de noticias por prioridad (ALTA, MEDIA, BAJA)
"""

import asyncio, json, logging, time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup
from cachetools import TTLCache
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ── Configuración ──────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SOURCES_FILE       = Path(__file__).parent / "sources.json"
TIMEOUT_POR_FUENTE = 10      # segundos por fuente
TIMEOUT_GLOBAL     = 30      # segundos por endpoint
DIAS_ATRAS         = 3       # filtro temporal
CACHE_TTL          = 3600    # 1 hora

cache: dict[str, TTLCache] = {
    "ALTA":  TTLCache(maxsize=1, ttl=CACHE_TTL),
    "MEDIA": TTLCache(maxsize=1, ttl=CACHE_TTL),
    "BAJA":  TTLCache(maxsize=1, ttl=CACHE_TTL),
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ── Carga de fuentes ───────────────────────────────────────
def cargar_fuentes() -> dict:
    if not SOURCES_FILE.exists():
        logger.warning("No se encontró sources.json. Usando fuentes vacías.")
        return {"ALTA": [], "MEDIA": [], "BAJA": []}
    with open(SOURCES_FILE, encoding="utf-8") as f:
        data = json.load(f)
    total = sum(len(v) for v in data.values())
    logger.info(f"Fuentes cargadas: {total} en {list(data.keys())}")
    return data

FUENTES = cargar_fuentes()

# ── Modelos Pydantic ───────────────────────────────────────
class NoticiaItem(BaseModel):
    fuente: str
    titulo: str
    url: str
    fecha_publicacion: Optional[str] = None
    fecha_detectada: str

class ErrorItem(BaseModel):
    fuente: str
    error: str

class ScrapingResponse(BaseModel):
    prioridad: str
    timestamp: str
    tiempo_ejecucion_ms: int
    total_noticias: int
    noticias: list[NoticiaItem]
    errores: list[ErrorItem]

# ── Lógica de scraping ─────────────────────────────────────
def construir_url(href: str, base: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return urljoin(base, href)

def fecha_es_reciente(fecha_str: Optional[str]) -> bool:
    if not fecha_str:
        return True  # sin fecha → incluir para no perder noticias
    try:
        fecha = datetime.fromisoformat(fecha_str[:10])
        return fecha >= datetime.now() - timedelta(days=DIAS_ATRAS)
    except Exception:
        return True

async def scrapear_fuente(
    session: aiohttp.ClientSession,
    fuente: dict,
    fecha_hoy: str,
) -> tuple[list[NoticiaItem], Optional[ErrorItem]]:

    nombre   = fuente.get("nombre", "?")
    url_base = fuente.get("url", "")
    selector = fuente.get("selector", "a")

    try:
        timeout = aiohttp.ClientTimeout(total=TIMEOUT_POR_FUENTE)
        async with session.get(url_base, timeout=timeout) as resp:
            if resp.status != 200:
                return [], ErrorItem(fuente=nombre, error=f"HTTP {resp.status}")
            html = await resp.text(errors="replace")

        soup    = BeautifulSoup(html, "html.parser")
        enlaces = soup.select(selector)

        noticias, vistos = [], set()
        for a in enlaces:
            href = a.get("href", "").strip()
            if not href or href.startswith(("#", "javascript")):
                continue
            url_noticia = construir_url(href, url_base)
            if url_noticia in vistos:
                continue
            vistos.add(url_noticia)

            titulo = a.get_text(strip=True) or a.get("title", "") or "Sin título"
            if len(titulo) < 6:
                continue

            # Intentar detectar fecha en el elemento padre
            fecha_pub = None
            padre = a.find_parent(["article", "div", "li", "section"])
            if padre:
                t = padre.find("time")
                if t:
                    fecha_pub = (t.get("datetime") or t.text.strip())[:10]

            if not fecha_es_reciente(fecha_pub):
                continue

            noticias.append(NoticiaItem(
                fuente=nombre,
                titulo=titulo[:200],
                url=url_noticia,
                fecha_publicacion=fecha_pub,
                fecha_detectada=fecha_hoy,
            ))

        return noticias, None

    except asyncio.TimeoutError:
        return [], ErrorItem(fuente=nombre, error="Timeout")
    except aiohttp.ClientConnectorError as e:
        return [], ErrorItem(fuente=nombre, error=f"Conexion fallida: {str(e)[:80]}")
    except Exception as e:
        return [], ErrorItem(fuente=nombre, error=str(e)[:150])


async def scrapear_prioridad(prioridad: str) -> ScrapingResponse:
    t_inicio   = time.monotonic()
    fecha_hoy  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    timestamp  = datetime.now(timezone.utc).isoformat()
    fuentes    = FUENTES.get(prioridad, [])

    if not fuentes:
        return ScrapingResponse(
            prioridad=prioridad, timestamp=timestamp,
            tiempo_ejecucion_ms=0, total_noticias=0, noticias=[],
            errores=[ErrorItem(fuente="sistema", error=f"Sin fuentes para {prioridad}")],
        )

    connector = aiohttp.TCPConnector(limit=20, ssl=False)
    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
        tareas = [scrapear_fuente(session, f, fecha_hoy) for f in fuentes]
        try:
            resultados = await asyncio.wait_for(
                asyncio.gather(*tareas, return_exceptions=True),
                timeout=TIMEOUT_GLOBAL,
            )
        except asyncio.TimeoutError:
            resultados = []

    todas_noticias, todos_errores = [], []
    for r in resultados:
        if isinstance(r, Exception):
            todos_errores.append(ErrorItem(fuente="desconocida", error=str(r)))
        else:
            noticias, error = r
            todas_noticias.extend(noticias)
            if error:
                todos_errores.append(error)

    # Deduplicar por URL
    urls_vistas, noticias_unicas = set(), []
    for n in todas_noticias:
        if n.url not in urls_vistas:
            urls_vistas.add(n.url)
            noticias_unicas.append(n)

    ms = int((time.monotonic() - t_inicio) * 1000)
    logger.info(f"[{prioridad}] {len(noticias_unicas)} noticias | {len(todos_errores)} errores | {ms}ms")

    return ScrapingResponse(
        prioridad=prioridad, timestamp=timestamp,
        tiempo_ejecucion_ms=ms, total_noticias=len(noticias_unicas),
        noticias=noticias_unicas, errores=todos_errores,
    )

# ── FastAPI app ────────────────────────────────────────────
app = FastAPI(
    title="News Scraper API",
    description="Scraping de noticias por prioridad para n8n",
    version="2.0.0",
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"])

async def endpoint_prioridad(prioridad: str, refresh: bool) -> ScrapingResponse:
    if not refresh and "data" in cache[prioridad]:
        logger.info(f"[{prioridad}] Cache hit")
        return cache[prioridad]["data"]
    resultado = await scrapear_prioridad(prioridad)
    cache[prioridad]["data"] = resultado
    return resultado

@app.get("/", tags=["info"])
async def root():
    return {
        "servicio": "News Scraper API", "version": "2.0.0",
        "endpoints": ["/ALTA", "/MEDIA", "/BAJA", "/todas", "/health"],
        "fuentes": {p: len(f) for p, f in FUENTES.items()},
        "cache_ttl_seg": CACHE_TTL,
    }

@app.get("/ALTA", response_model=ScrapingResponse, tags=["scraping"])
async def get_alta(refresh: bool = Query(False)):
    return await endpoint_prioridad("ALTA", refresh)

@app.get("/MEDIA", response_model=ScrapingResponse, tags=["scraping"])
async def get_media(refresh: bool = Query(False)):
    return await endpoint_prioridad("MEDIA", refresh)

@app.get("/BAJA", response_model=ScrapingResponse, tags=["scraping"])
async def get_baja(refresh: bool = Query(False)):
    return await endpoint_prioridad("BAJA", refresh)

@app.get("/todas", tags=["scraping"])
async def get_todas(refresh: bool = Query(False)):
    r = await asyncio.gather(
        endpoint_prioridad("ALTA", refresh),
        endpoint_prioridad("MEDIA", refresh),
        endpoint_prioridad("BAJA", refresh),
    )
    return {"ALTA": r[0].model_dump(), "MEDIA": r[1].model_dump(), "BAJA": r[2].model_dump()}

@app.get("/health", tags=["info"])
async def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}

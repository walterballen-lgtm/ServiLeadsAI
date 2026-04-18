"""
app.py — ServiLeads AI v3.8
Interfaz conversacional: el agente guía al usuario campo por campo.
La lógica de extracción (Apollo/Lusha) corre por debajo sin exponerse.

=============================================================
PARA AGREGAR UN NUEVO CONECTOR:
  1. Agrega un dict a CONNECTORS
  2. Agrega el elif en _run_job()
  3. El bot lo ofrece automáticamente en el menú inicial
=============================================================
"""

import os, uuid, queue, threading, tempfile, csv, secrets, json, re, unicodedata
import requests
from functools import wraps
from urllib.parse import urlencode
from flask import (
    Flask, render_template, request, jsonify,
    Response, send_file, stream_with_context,
    redirect, url_for, session as flask_session,
)

import apollo_script, lusha_script, apollo_org, lusha_org
try:
    import google.generativeai as genai
    _GENAI_OK = True
except ImportError:
    _GENAI_OK = False

try:
    import shapefile as pyshp
    _SHP_OK = True
except ImportError:
    _SHP_OK = False

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "servi-leads-ai-2024")
app.config["JSON_AS_ASCII"] = False

# Render (y la mayoría de plataformas cloud) corren detrás de un proxy HTTPS.
# Sin esto, request.url_root devuelve "http://" y Google rechaza el redirect.
from werkzeug.middleware.proxy_fix import ProxyFix
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

# ================================================================
# API KEYS — configúralas en Render → Environment Variables
# (nunca las pongas directamente en el código)
# ================================================================
APOLLO_API_KEY = os.environ.get("APOLLO_API_KEY", "")
LUSHA_API_KEY  = os.environ.get("LUSHA_API_KEY",  "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_URL     = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"

# ================================================================
# GOOGLE OAUTH — configura en Render → Environment Variables
# ================================================================
GOOGLE_CLIENT_ID     = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REDIRECT_URI  = os.environ.get("GOOGLE_REDIRECT_URI", "")  # ej: https://tu-app.onrender.com/auth/callback

GOOGLE_AUTH_URL  = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_INFO_URL  = "https://www.googleapis.com/oauth2/v3/userinfo"

ALLOWED_COMPANIES = ["SERVINFORMACION", "Saving the amazon", "CNID", "ProaIA", "Geodatamaps"]


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        user = flask_session.get("user")
        if not user:
            return redirect(url_for("login"))
        if not user.get("company"):
            return redirect(url_for("profile"))
        return f(*args, **kwargs)
    return decorated


# ================================================================
# CONECTORES — edita aquí para agregar botones / nuevas APIs
# ================================================================
CONNECTORS = [
    {
        "id": "BUSQUEDA_CONTACTOS",
        "label": "Búsqueda de Contactos",
        "emoji": "🚀",
        "color": "#1f6feb",
        "required_files": ["empresas_file", "cargos_file"],
        "needs_countries": True,
        "output_filename": "Contactos_ServiLeads.csv",
    },
]

PAISES_MAPEO = {
    "Norteamérica": {
        "Estados Unidos": "United States",
        "Canadá": "Canada",
        "México": "Mexico",
    },
    "Centroamérica": {
        "Belice": "Belize",
        "Costa Rica": "Costa Rica",
        "El Salvador": "El Salvador",
        "Guatemala": "Guatemala",
        "Honduras": "Honduras",
        "Nicaragua": "Nicaragua",
        "Panamá": "Panama",
    },
    "Suramérica": {
        "Argentina": "Argentina",
        "Bolivia": "Bolivia",
        "Brasil": "Brazil",
        "Chile": "Chile",
        "Colombia": "Colombia",
        "Ecuador": "Ecuador",
        "Guyana": "Guyana",
        "Paraguay": "Paraguay",
        "Perú": "Peru",
        "Surinam": "Suriname",
        "Uruguay": "Uruguay",
        "Venezuela": "Venezuela",
    },
    "Caribe": {
        "Antigua y Barbuda": "Antigua and Barbuda",
        "Bahamas": "Bahamas",
        "Barbados": "Barbados",
        "Bermudas": "Bermuda",
        "Dominica": "Dominica",
        "República Dominicana": "Dominican Republic",
        "Granada": "Grenada",
        "Guadalupe": "Guadeloupe",
        "Haití": "Haiti",
        "Jamaica": "Jamaica",
        "Martinica": "Martinique",
        "Montserrat": "Montserrat",
        "Puerto Rico": "Puerto Rico",
        "San Cristóbal y Nieves": "Saint Kitts and Nevis",
        "Santa Lucía": "Saint Lucia",
        "San Vicente y las Granadinas": "Saint Vincent and the Grenadines",
        "Trinidad y Tobago": "Trinidad and Tobago",
        "Islas Turcas y Caicos": "Turks and Caicos Islands",
    },
}


# ================================================================
# SHAPEFILE → GEOJSON (cargado al inicio)
# ================================================================
def _load_geojson() -> dict:
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "puntos_empresas", "demo_info")
    if not _SHP_OK:
        return {"type": "FeatureCollection", "features": []}
    sf = pyshp.Reader(path, encoding="utf-8")
    fields = [f[0] for f in sf.fields[1:]]
    features = []
    for shp, rec in zip(sf.shapes(), sf.records()):
        props = {k: (v.strip() if isinstance(v, str) else v) for k, v in zip(fields, rec)}
        features.append({"type": "Feature", "geometry": shp.__geo_interface__, "properties": props})
    return {"type": "FeatureCollection", "features": features}

try:
    GEOJSON_CACHE = _load_geojson()
except Exception as _e:
    print(f"[shapefile] Error al cargar: {_e}")
    GEOJSON_CACHE = {"type": "FeatureCollection", "features": []}


# ================================================================
# STORES EN MEMORIA
# ================================================================
conversations: dict = {}   # sid  → ConvState
jobs: dict = {}            # job_id → job data


# ================================================================
# ESTADO DE CONVERSACIÓN
# ================================================================
class ConvState:
    """Guarda el progreso de la conversación de un usuario."""

    STEP_MAP = {
        "BUSQUEDA_CONTACTOS": ["ask_empresas", "ask_cargos", "ask_countries", "confirm"],
    }

    def __init__(self, sid: str):
        self.sid = sid
        self.step = "welcome"
        self.process_type: str = None
        self.paises: list = []
        self.paises_names: list = []
        self.empresas_path: str = None
        self.cargos_path: str = None
        self.empresas_count: int = 0
        self.cargos_count: int = 0
        self.job_id: str = None
        self.upload_dir = tempfile.mkdtemp(prefix=f"conv_{sid[:8]}_")
        self.pending_confirm_file: dict = None
        self.gemini_history: list = []

    def advance(self):
        steps = self.STEP_MAP.get(self.process_type, [])
        if self.step in steps:
            idx = steps.index(self.step)
            if idx + 1 < len(steps):
                self.step = steps[idx + 1]

    def summary_items(self) -> list:
        items = [("Proceso", "🚀 Búsqueda de Contactos (Apollo → Lusha → Consolidado)")]
        if self.empresas_path:
            items.append(("Empresas", f"{self.empresas_count} registros"))
        if self.cargos_path:
            items.append(("Cargos", f"{self.cargos_count} registros"))
        if self.paises_names:
            shown = self.paises_names[:4]
            extra = len(self.paises_names) - 4
            label = ", ".join(shown) + (f" +{extra} más" if extra > 0 else "")
            items.append(("Países", label))
        items.append(("APIs", "Apollo + Lusha (variables de entorno)"))
        return items


# ================================================================
# HELPERS
# ================================================================
def count_csv_rows(path: str) -> int:
    for enc in ["latin-1", "utf-8-sig", "utf-8"]:
        try:
            with open(path, "r", encoding=enc) as f:
                return max(0, sum(1 for _ in f) - 1)
        except (UnicodeDecodeError, OSError):
            continue
    return 0


def _get_csv_header(path: str) -> str:
    """Devuelve el encabezado de la primera columna del CSV."""
    for enc in ["latin-1", "utf-8-sig", "utf-8"]:
        try:
            with open(path, "r", encoding=enc) as f:
                reader = csv.reader(f)
                row = next(reader, None)
                if row and row[0].strip():
                    return row[0].strip()
                return ""
        except UnicodeDecodeError:
            continue
    return ""



# ================================================================
# GEMINI AI
# ================================================================
_GEMINI_SYSTEM = """\
Eres ServiLeads AI, asistente de extracción de contactos.

REGLA CRÍTICA: Responde ÚNICAMENTE con un objeto JSON. Sin texto extra antes ni después.
Formato obligatorio:
{{"message": "<texto markdown>", "action": "<accion_o_null>", "params": {{}}}}

ACCIONES DISPONIBLES:
- "start_process" → Iniciar extracción. params: {{"process_type": "BUSQUEDA_CONTACTOS"}}
- "download_data" → Descargar CSV. params: {{"pais": "<pais_o_null>", "empresa": "<nombre_o_null>"}}
- "show_summary"  → Resumen general del mapa. params: {{}}
- "filter_map"    → Filtrar mapa Y mostrar contactos en chat. params: {{"pais": "<pais_o_null>", "empresa": "<nombre_o_null>"}}
- "search_map"    → Buscar registro específico. params: {{"query": "<texto>", "field": "nombre"|"empresa"|"cargo"|"correo", "empresa_filter": "<empresa_o_null>", "pais": "<pais_o_null>"}}
- null            → Solo responder con texto

CUÁNDO USAR CADA ACCIÓN:
- "filter_map": el usuario pregunta por contactos DE un país o empresa ("qué tengo en Colombia", "contactos de Alpina").
- "search_map": el usuario busca un registro específico ("existe Manuel", "gerente de Alpina", "busca la empresa RAMO").
- "download_data": el usuario quiere descargar el CSV.
- "start_process": el usuario quiere extraer contactos nuevos.

PROCESO DE EXTRACCIÓN:
- BUSQUEDA_CONTACTOS → Proceso unificado: busca en Apollo primero, luego en Lusha para empresas sin resultados, consolida todo y valida cargos con IA. Requiere: CSV empresas, CSV cargos, países.

BASE DE DATOS DEL MAPA:
{map_summary}
Campos: pais, empresa, nombre, cargo, correo, telefono, url

ESTADO CONVERSACIÓN:
{conv_state}

REGLAS:
- "gerente de Alpina" → search_map, query="gerente", field="cargo", empresa_filter="Alpina"
- "busca empresa RAMO" → search_map, query="RAMO", field="empresa", empresa_filter=null
- "existe Manuel" → search_map, query="Manuel", field="nombre", empresa_filter=null
- "contactos en Colombia" → filter_map, pais="Colombia"
- Sé conciso. Usa **negritas** para datos importantes.\
"""


def _extract_json(text: str) -> tuple[dict, str]:
    """Extrae el primer objeto JSON del texto. Retorna (dict, error_str)."""
    # Intentar parsear directamente primero
    try:
        result = json.loads(text.strip())
        if isinstance(result, dict):
            return result, ""
    except Exception:
        pass
    # Buscar bloque ```json ... ``` o ``` ... ```
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if m:
        try:
            result = json.loads(m.group(1))
            if isinstance(result, dict):
                return result, ""
        except Exception as e:
            return {}, f"JSON en bloque de código inválido: {e}"
    # Buscar el primer { ... } más externo
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if m:
        try:
            result = json.loads(m.group(0))
            if isinstance(result, dict):
                return result, ""
        except Exception as e:
            return {}, f"JSON encontrado pero inválido: {e}\nTexto raw: {text[:300]}"
    return {}, f"No se encontró JSON en la respuesta de Gemini.\nRespuesta raw: {text[:300]}"


def _gemini_context(conv: "ConvState") -> str:
    feats = GEOJSON_CACHE.get("features", [])
    from collections import Counter
    by_pais = Counter(f["properties"].get("pais", "?") for f in feats)
    summary = f"{len(feats)} registros totales — " + ", ".join(
        f"{p}: {c}" for p, c in sorted(by_pais.items(), key=lambda x: -x[1])
    )
    state = f"step={conv.step}"
    if conv.process_type: state += f", proceso={conv.process_type}"
    if conv.empresas_count: state += f", empresas_cargadas={conv.empresas_count}"
    if conv.cargos_count:   state += f", cargos_cargados={conv.cargos_count}"
    if conv.paises_names:   state += f", países={conv.paises_names}"
    return _GEMINI_SYSTEM.format(map_summary=summary, conv_state=state)


def call_gemini(conv: "ConvState", user_message: str) -> dict:
    """
    Llama a Gemini. Siempre devuelve dict con claves: message, action, params, _error.
    _error contiene descripción detallada si algo falló (para mostrar en chat en debug).
    """
    empty = {"message": None, "action": None, "params": {}, "_error": None}

    if not GEMINI_API_KEY:
        empty["_error"] = "GEMINI_API_KEY no configurada en variables de entorno."
        return empty

    conv.gemini_history.append({"role": "user", "parts": [{"text": user_message}]})
    body = {
        "system_instruction": {"parts": [{"text": _gemini_context(conv)}]},
        "contents": conv.gemini_history[-20:],
        "generationConfig": {"temperature": 0.3, "maxOutputTokens": 600},
    }
    try:
        resp = requests.post(GEMINI_URL, params={"key": GEMINI_API_KEY}, json=body, timeout=20)
        if resp.status_code != 200:
            conv.gemini_history.pop()
            empty["_error"] = f"Gemini HTTP {resp.status_code}: {resp.text[:400]}"
            return empty

        raw = resp.json()
        candidates = raw.get("candidates", [])
        if not candidates:
            conv.gemini_history.pop()
            empty["_error"] = f"Gemini sin candidatos. Respuesta: {str(raw)[:400]}"
            return empty

        raw_text = candidates[0].get("content", {}).get("parts", [{}])[0].get("text") or ""
        if not raw_text:
            conv.gemini_history.pop()
            empty["_error"] = f"Gemini devolvió texto vacío. Candidato: {str(candidates[0])[:300]}"
            return empty
        result, parse_error = _extract_json(raw_text)

        if parse_error:
            conv.gemini_history.pop()
            empty["_error"] = parse_error
            return empty

        conv.gemini_history.append({"role": "model", "parts": [{"text": raw_text}]})
        return {
            "message": result.get("message") or None,
            "action":  result.get("action")  or None,
            "params":  result.get("params")  if isinstance(result.get("params"), dict) else {},
            "_error":  None,
        }
    except requests.exceptions.Timeout:
        conv.gemini_history.pop()
        empty["_error"] = "Gemini tardó más de 20 segundos (timeout)."
        return empty
    except Exception as e:
        if conv.gemini_history and conv.gemini_history[-1]["role"] == "user":
            conv.gemini_history.pop()
        empty["_error"] = f"Error inesperado llamando a Gemini: {e}"
        return empty


# ================================================================
# BÚSQUEDA Y FILTRO EN MAPA
# ================================================================
def _norm(s: str) -> str:
    """Minúsculas + sin acentos para comparaciones flexibles."""
    return unicodedata.normalize("NFD", str(s).lower()).encode("ascii", "ignore").decode()


def resp_search_map(query: str, field: str = "nombre", empresa_filter: str = "", pais: str = "") -> list:
    feats = GEOJSON_CACHE.get("features", [])
    query = query or ""
    q = _norm(query.strip())
    if field not in {"nombre", "empresa", "cargo", "correo", "url", "telefono"}:
        field = "nombre"

    # Si hay filtro de empresa, verificar primero que exista en la base
    if empresa_filter:
        ef = _norm(empresa_filter)
        empresa_exists = any(ef in _norm(f["properties"].get("empresa", "")) for f in feats)
        if not empresa_exists:
            return [_text(f"No tienes la empresa **{empresa_filter}** en tu base de datos.")]

    # Buscar con todos los filtros (parcial + sin acentos)
    matches = [
        f for f in feats
        if q in _norm(f["properties"].get(field, ""))
        and (not pais           or _norm(pais) in _norm(f["properties"].get("pais", "")))
        and (not empresa_filter or _norm(empresa_filter) in _norm(f["properties"].get("empresa", "")))
    ]

    if not matches:
        # ¿Existe en otros países?
        matches_other = [
            f for f in feats
            if q in _norm(f["properties"].get(field, ""))
            and (not empresa_filter or _norm(empresa_filter) in _norm(f["properties"].get("empresa", "")))
        ]
        if matches_other and pais:
            countries = sorted({f["properties"].get("pais", "") for f in matches_other if f["properties"].get("pais")})
            return [
                _text(f"No encontré **\"{query}\"** en {field} en **{pais}**, pero sí en: {', '.join(countries)}."),
                _replies(
                    [{"label": f"{_FLAGS.get(c,'🌍')} Buscar en {c}", "value": f"SEARCH:{field}:{query}:{empresa_filter}:{c}"}
                     for c in countries] +
                    [{"label": "🌍 Ver en todos los países", "value": f"SEARCH:{field}:{query}:{empresa_filter}:ALL"}]
                ),
            ]
        if empresa_filter:
            return [_text(f"La empresa **{empresa_filter}** existe en tu base, pero no tiene ningún contacto con **\"{query}\"** como {field}.")]
        scope = f" en **{pais}**" if pais else ""
        return [_text(f"No encontré ningún registro con **\"{query}\"** en el campo *{field}*{scope}.")]

    # Mostrar resultados
    scope = ""
    if empresa_filter: scope += f" en **{empresa_filter}**"
    if pais:           scope += f" ({pais})"
    total = len(matches)
    shown = matches[:30]
    lines = [f"🔍 **{total} resultado(s)** para \"{query}\"{scope}:\n"]
    for feat in shown:
        p = feat["properties"]
        lines.append(f"• **{p.get('nombre','?')}** — {p.get('cargo','?')}")
        lines.append(f"  🏢 {p.get('empresa','?')} ({p.get('pais','?')})")
        lines.append(f"  📧 {p.get('correo') or 'No disponible'}")
        lines.append(f"  📞 {p.get('telefono') or 'No disponible'}")

    msgs = [_text("\n".join(lines))]

    # Si hay más de 30, ofrecer descargar el total
    if total > 30:
        qs = []
        if empresa_filter: qs.append(f"empresa={empresa_filter}")
        if pais:           qs.append(f"pais={pais}")
        qs.append(f"q={query}&field={field}")
        url = "/download?" + "&".join(qs)
        msgs.append(_text(f"_Mostrando 30 de {total}. Descarga para ver todos:_"))
        msgs.append(_link(url, f"⬇️ Descargar los {total} resultados"))

    # Ofrecer buscar en otros países si hay resultados fuera del filtro actual
    if pais:
        total_global = sum(1 for f in feats
                    if q in _norm(f["properties"].get(field, ""))
                    and (not empresa_filter or _norm(empresa_filter) in _norm(f["properties"].get("empresa", ""))))
        if total_global > total:
            msgs.append(_replies([{"label": f"🌍 Ver en todos los países ({total_global} total)", "value": f"SEARCH:{field}:{query}:{empresa_filter}:ALL"}]))
    return msgs


def resp_filter_map(pais: str = "", empresa: str = "", extra_msg: str = "") -> list:
    """Filtra el mapa visualmente y muestra resumen de contactos en el chat."""
    from collections import Counter
    feats = GEOJSON_CACHE.get("features", [])
    filtered = [f for f in feats if
        (not pais    or _norm(pais)    in _norm(f["properties"].get("pais",    ""))) and
        (not empresa or _norm(empresa) in _norm(f["properties"].get("empresa", "")))]

    scope = f"**{pais}**" if pais else f"**{empresa}**" if empresa else "todos los países"
    map_msg = {"type": "map_action", "action": "filter", "pais": pais or None, "empresa": empresa or None}

    if not filtered:
        return [_text(f"No tienes registros en {scope} en tu base de datos."), map_msg]

    by_empresa = Counter(f["properties"].get("empresa", "?") for f in filtered)
    by_cargo   = Counter(f["properties"].get("cargo",   "?") for f in filtered)

    lines = [f"📊 **{len(filtered)} contactos** en {scope}:\n"]
    shown = filtered[:30]
    for f in shown:
        p = f["properties"]
        nombre = p.get("nombre", "—")
        cargo  = p.get("cargo",  "")
        emp    = p.get("empresa","")
        correo = p.get("correo", "")
        linea  = f"• **{nombre}**"
        if cargo:  linea += f" — {cargo}"
        if emp:    linea += f" | {emp}"
        if correo: linea += f" | {correo}"
        lines.append(linea)
    if len(filtered) > 30:
        lines.append(f"\n_...y {len(filtered)-30} más. Descarga el CSV para verlos todos._")

    qs, lp = [], []
    if pais:    qs.append(f"pais={pais}");       lp.append(pais)
    if empresa: qs.append(f"empresa={empresa}"); lp.append(empresa)
    url   = "/api/download-map?" + "&".join(qs) if qs else "/api/download-map"
    label = " — ".join(lp) if lp else "todos"

    msgs = []
    if extra_msg: msgs.append(_text(extra_msg))
    msgs += [_text("\n".join(lines)), map_msg, _link(url, f"⬇️ Descargar {label} ({len(filtered)} registros)")]
    return msgs


def leer_csv_primera_columna(path: str) -> list:
    for enc in ["latin-1", "utf-8-sig", "utf-8"]:
        try:
            result = []
            with open(path, "r", encoding=enc) as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if row and row[0].strip():
                        result.append(row[0].strip()[:500])
            return result
        except UnicodeDecodeError:
            continue
    return []


# ---- Constructores de mensajes del agente ----
def _text(content: str) -> dict:
    return {"type": "text", "content": content}

def _replies(items: list) -> dict:
    return {"type": "quick_replies", "items": items}

def _upload(field: str, label: str, hint: str = "") -> dict:
    return {"type": "file_upload", "field": field, "label": label, "hint": hint}

def _countries() -> dict:
    return {"type": "countries_picker", "paises": PAISES_MAPEO}

def _summary(items: list) -> dict:
    return {"type": "summary", "items": items}

def _stream(job_id: str) -> dict:
    return {"type": "stream_start", "job_id": job_id}

def _download(job_id: str) -> dict:
    return {"type": "download", "job_id": job_id}


# ================================================================
# MENSAJES POR PASO (flujo guiado)
# ================================================================
def _link(url: str, label: str) -> dict:
    return {"type": "download_link", "url": url, "label": label}


def _process_menu() -> dict:
    return _replies([{"label": f"{c['emoji']} {c['label']}", "value": c["id"]} for c in CONNECTORS])


def step_messages(conv: "ConvState") -> list:
    step = conv.step
    if step == "ask_empresas":
        return [
            _text("Para empezar necesito el **CSV de Empresas** — una empresa por fila en la primera columna."),
            _upload("empresas_file", "📎 Subir CSV de Empresas", "Primera columna = nombre de empresa"),
        ]
    if step == "ask_cargos":
        return [
            _text("Ahora el **CSV de Cargos** — el título de los puestos que quieres buscar (ej: CEO, Director Comercial)."),
            _upload("cargos_file", "📎 Subir CSV de Cargos", "Primera columna = cargo/título"),
        ]
    if step == "ask_id_org":
        return [
            _text("Necesito el **CSV de IDs de Organizaciones** — el ID de cada organización en la plataforma."),
            _upload("id_org_file", "📎 Subir CSV de Id Organizaciones", "Primera columna = ID de organización"),
        ]
    if step == "ask_countries":
        return [
            _text("Casi listo. ¿En qué **países** quieres enfocar la búsqueda?"),
            _countries(),
        ]
    if step == "confirm":
        return [
            _text("✅ Tengo todo. Revisa el resumen y haz clic en **Iniciar búsqueda** cuando estés listo."),
            _summary(conv.summary_items()),
        ]
    return [_text("Escribe *reiniciar* para empezar de nuevo.")]


# ================================================================
# DETECCIÓN DE INTENCIÓN (sin API externa)
# ================================================================
_GREET_KW    = {"hola", "buenas", "hey", "saludos", "buenos días", "buenas tardes",
                "hi", "buen día", "good morning", "good afternoon"}
_HELP_KW     = {"qué puedes", "que puedes", "qué haces", "que haces", "ayuda", "help",
                "para qué sirves", "para que sirves", "funciones", "capacidades",
                "cómo funciona", "como funciona", "qué eres", "que eres",
                "qué ofreces", "que ofreces"}
_DATA_KW     = {"qué datos", "que datos", "mostrar datos", "ver datos",
                "cuántos registros", "cuantos registros", "resumen de datos",
                "qué hay en el mapa", "que hay en el mapa", "registros disponibles",
                "qué información tengo", "que información tengo", "qué tengo", "que tengo",
                "qué info", "que info", "base de datos"}
_DOWNLOAD_KW = {"descargar", "descarga", "exportar", "exporta",
                "dame los datos", "dame los contactos", "bajar datos", "download"}
_DESCRIBE_KW = {"describe", "cuéntame sobre", "cuéntame de", "cuentame sobre", "cuentame de",
                "qué es apollo", "que es apollo", "qué es lusha", "que es lusha",
                "explícame", "explicame", "explica", "más información sobre",
                "mas información sobre", "cómo se usa", "como se usa", "detalles de"}
_START_KW    = {"quiero buscar", "iniciar búsqueda", "iniciar busqueda",
                "hacer una búsqueda", "hacer una busqueda", "comenzar búsqueda",
                "extraer contactos", "usar apollo", "usar lusha", "lanzar búsqueda",
                "empezar búsqueda", "quiero extraer"}


def detect_intent(text: str) -> str:
    t = text.lower().strip()
    if len(t) < 30 and any(k in t for k in _GREET_KW):
        return "greeting"
    if any(k in t for k in _HELP_KW):
        return "help"
    if any(k in t for k in _DATA_KW):
        return "show_data"
    if any(k in t for k in _DOWNLOAD_KW):
        return "download"
    if any(k in t for k in _DESCRIBE_KW):
        return "describe"
    if any(k in t for k in _START_KW):
        return "start"
    return "unknown"


# ================================================================
# RESPUESTAS POR INTENCIÓN
# ================================================================
_FLAGS = {"Colombia": "🇨🇴", "Peru": "🇵🇪", "Uruguay": "🇺🇾"}

_PAIS_ALIAS = {
    "colombia": "Colombia",
    "peru": "Peru", "perú": "Peru",
    "uruguay": "Uruguay",
}


def _mid_flow_note(conv: "ConvState") -> list:
    """Recordatorio suave cuando estamos en medio de un flujo guiado."""
    label_map = {
        "ask_empresas":  "subir el CSV de empresas",
        "ask_cargos":    "subir el CSV de cargos",
        "ask_id_org":    "subir el CSV de IDs",
        "ask_countries": "seleccionar países",
        "confirm":       "confirmar e iniciar la búsqueda",
    }
    if conv.step in label_map:
        proceso = conv.process_type.replace("_", " ").title() if conv.process_type else "búsqueda"
        return [_text(f"_(Seguimos con la búsqueda de **{proceso}** — pendiente: {label_map[conv.step]}.)_")]
    return []


def resp_greeting() -> list:
    return [_text(
        "¡Hola! 👋 Bienvenido a **ServiLeads AI**.\n\n"
        "Soy **Junno** tu asistente para extracción de contactos. Puedo:\n\n"
        "• 🚀 **Extraer contactos** — Multiplataforma en cascada, consolidado y validado con IA\n"
        "• 📊 Mostrarte los **datos del mapa** y filtrar por país o empresa\n"
        "• ⬇️ **Descargar** registros por país o empresa\n\n"
        "¿En qué te puedo ayudar hoy?"
    )]


def resp_help(conv: "ConvState") -> list:
    feats = GEOJSON_CACHE.get("features", [])
    msgs = [_text(
        "**¿Qué puede hacer ServiLeads AI?** 🤖\n\n"
        "**🚀 Búsqueda de Contactos (proceso unificado):**\n"
        "1. Busca en **Apollo** primero para todas tus empresas\n"
        "2. Las empresas sin resultados van a **Lusha** automáticamente\n"
        "3. Consolida ambos resultados en un solo archivo\n"
        "4. Valida los cargos con **IA (Gemini)** para entregarte solo contactos relevantes\n\n"
        "**Necesitas:** CSV de empresas + CSV de cargos + selección de países\n\n"
        f"**Datos del mapa:** {len(feats)} registros disponibles para explorar y descargar.\n\n"
        "Escribe *'quiero buscar contactos'* para iniciar, o *'qué datos tengo'* para explorar el mapa."
    )]
    return msgs + _mid_flow_note(conv)


def resp_show_data(conv: "ConvState") -> list:
    from collections import Counter
    feats = GEOJSON_CACHE.get("features", [])
    if not feats:
        return [_text("No hay datos disponibles en el mapa en este momento.")]

    by_pais    = Counter(f["properties"].get("pais", "?")    for f in feats)
    by_empresa = Counter(f["properties"].get("empresa", "?") for f in feats)

    lines = [f"📊 **Datos en el mapa — {len(feats)} registros totales**\n"]
    lines.append("**Por país:**")
    for pais, cnt in sorted(by_pais.items(), key=lambda x: -x[1]):
        lines.append(f"  {_FLAGS.get(pais, '🌍')} {pais}: **{cnt}** contactos")
    lines.append("\n**Por empresa:**")
    for emp, cnt in sorted(by_empresa.items(), key=lambda x: -x[1]):
        lines.append(f"  🏢 {emp}: {cnt}")
    lines.append("\n**Campos disponibles:** país · empresa · nombre · cargo · correo · teléfono · LinkedIn")
    lines.append("\n¿Quieres **descargar** los datos de algún país o empresa específica?")

    return [_text("\n".join(lines))] + _mid_flow_note(conv)


def resp_download(text: str, conv: "ConvState") -> list:
    t = text.lower()
    feats = GEOJSON_CACHE.get("features", [])

    found_pais    = next((v for k, v in _PAIS_ALIAS.items() if k in t), None)
    all_companies = list({f["properties"].get("empresa", "") for f in feats if f["properties"].get("empresa")})
    found_empresa = next(
        (emp for emp in all_companies
         if any(w in t for w in emp.lower().split() if len(w) >= 4)),
        None
    )

    if not found_pais and not found_empresa:
        countries = sorted({f["properties"].get("pais", "") for f in feats if f["properties"].get("pais")})
        return [
            _text("¿De qué país o empresa quieres descargar los datos?"),
            _replies(
                [{"label": f"🌍 Todos ({len(feats)})", "value": "DOWNLOAD_ALL"}] +
                [{"label": f"{_FLAGS.get(c,'🌍')} {c}", "value": f"DOWNLOAD_PAIS:{c}"}
                 for c in countries]
            ),
        ] + _mid_flow_note(conv)

    filtered = [f for f in feats if
        (not found_pais    or f["properties"].get("pais")    == found_pais) and
        (not found_empresa or f["properties"].get("empresa") == found_empresa)]

    if not filtered:
        return [_text("No encontré registros con esos filtros. Prueba otro país o empresa.")]

    params, label_parts = [], []
    if found_pais:    params.append(f"pais={found_pais}");       label_parts.append(found_pais)
    if found_empresa: params.append(f"empresa={found_empresa}"); label_parts.append(found_empresa)
    url   = "/api/download-map?" + "&".join(params)
    label = " — ".join(label_parts)

    return [
        _text(f"Encontré **{len(filtered)} registros** para {label}."),
        _link(url, f"⬇️ Descargar {label} ({len(filtered)} registros)"),
    ] + _mid_flow_note(conv)


def resp_describe(text: str, conv: "ConvState") -> list:
    desc = (
        "🚀 **Búsqueda de Contactos — Proceso Unificado**\n\n"
        "Combina Apollo y Lusha en una sola ejecución inteligente:\n\n"
        "**Fase 1 — Plataforma principal:** busca contactos para todas tus empresas\n"
        "**Fase 2 — Plataforma secundaria:** busca automáticamente las empresas sin resultados\n"
        "**Fase 3 — Consolidado:** une ambos resultados en un solo CSV\n"
        "**Fase 4 — Validación IA:** Gemini filtra los contactos según los cargos que buscas\n\n"
        "**Necesitas:**\n"
        "• CSV de empresas (una por fila)\n"
        "• CSV de cargos (uno por fila)\n"
        "• Selección de países\n\n"
        "**Obtienes:** `Contactos_ServiLeads.csv` — contactos validados con nombre, cargo, correo y teléfono."
    )
    return [_text(desc)] + _mid_flow_note(conv)


# ================================================================
# MOTOR DE CONVERSACIÓN
# ================================================================
RESET_WORDS = {"reiniciar", "restart", "reset"}


def handle_turn(sid: str, payload: dict) -> list:
    conv = conversations.get(sid)
    if conv is None:
        conv = ConvState(sid)
        conversations[sid] = conv

    ptype = payload.get("type", "text")
    value = str(payload.get("value", "")).strip()

    # Recarga de página → saludo + botón de búsqueda
    if ptype == "init":
        conv.step = "welcome"
        return resp_greeting() + [_replies([{"label": "🚀 Iniciar Búsqueda de Contactos", "value": "BUSQUEDA_CONTACTOS"}])]

    # Reinicio explícito
    if ptype == "text" and value.lower() in RESET_WORDS:
        conversations[sid] = ConvState(sid)
        return [_text("↩️ ¡Listo, empezamos de nuevo!")] + resp_greeting()

    # Botones de descarga generados dinámicamente
    if ptype == "action" and value.startswith("DOWNLOAD_"):
        feats = GEOJSON_CACHE.get("features", [])
        if value == "DOWNLOAD_ALL":
            return [
                _text(f"Aquí están todos los registros — **{len(feats)}** contactos."),
                _link("/api/download-map", f"⬇️ Descargar todos ({len(feats)} registros)"),
            ]
        if value.startswith("DOWNLOAD_PAIS:"):
            pais = value.split(":", 1)[1]
            cnt  = sum(1 for f in feats if f["properties"].get("pais") == pais)
            return [
                _text(f"**{cnt} registros** de {pais} listos para descargar."),
                _link(f"/api/download-map?pais={pais}", f"⬇️ Descargar {pais} ({cnt} registros)"),
            ]

    # Selección de proceso desde el menú
    if ptype == "action" and value in {c["id"] for c in CONNECTORS}:
        conv.process_type = value
        conv.step = conv.STEP_MAP[value][0]
        conn = next(c for c in CONNECTORS if c["id"] == value)
        return [_text(f"Perfecto, vamos con **{conn['label']}** {conn['emoji']}")] + step_messages(conv)

    # Confirmación de archivo sospechoso (Yes/No)
    if ptype == "action" and value.startswith("CONFIRM_FILE:"):
        parts = value.split(":", 1)[1]
        label_map = {"empresas_file": "Empresas", "cargos_file": "Cargos", "id_org_file": "Id Organizaciones"}
        if parts == "yes" and conv.pending_confirm_file:
            info = conv.pending_confirm_file
            conv.pending_confirm_file = None
            field = info["field"]
            # Asignar directamente sin re-validar cabecera
            if field == "empresas_file":
                conv.empresas_path  = info["path"]
                conv.empresas_count = info["count"]
            elif field == "cargos_file":
                conv.cargos_path  = info["path"]
                conv.cargos_count = info["count"]
            elif field == "id_org_file":
                conv.id_org_path  = info["path"]
                conv.id_org_count = info["count"]
            conv.advance()
            count = info["count"]
            return [_text(f"✅ {count} registros cargados."),
                    _replies([{"label": f"🔄 Reemplazar archivo", "value": f"REUPLOAD:{field}"}])
                    ] + step_messages(conv)
        elif parts == "no" and conv.pending_confirm_file:
            info = conv.pending_confirm_file
            conv.pending_confirm_file = None
            field = info["field"]
            return [
                _text("Entendido. Sube el archivo correcto 👇"),
                _upload(field, f"📎 Subir CSV de {label_map.get(field, field)}", "Primera columna = dato esperado"),
            ]

    # Re-subir archivo
    if ptype == "action" and value.startswith("REUPLOAD:"):
        field = value.split(":", 1)[1]
        label_map = {"empresas_file": "Empresas", "cargos_file": "Cargos", "id_org_file": "Id Organizaciones"}
        return [
            _text("Claro, sube el nuevo archivo y lo reemplazaré 👇"),
            _upload(field, f"📎 Reemplazar CSV de {label_map.get(field, field)}", "Primera columna = dato esperado"),
        ]

    # Búsqueda en mapa desde quick reply  SEARCH:field:query:empresa_filter:pais
    if ptype == "action" and value.startswith("SEARCH:"):
        parts = value.split(":", 4)
        s_field   = parts[1] if len(parts) > 1 else "nombre"
        s_query   = parts[2] if len(parts) > 2 else ""
        s_empresa = parts[3] if len(parts) > 3 else ""
        s_pais    = parts[4] if len(parts) > 4 else ""
        if s_pais    == "ALL": s_pais    = ""
        if s_empresa == "ALL": s_empresa = ""
        if s_query:
            return resp_search_map(s_query, s_field, s_empresa, s_pais)

    # Intención libre (texto) — Gemini primero, fallback a keywords
    if ptype == "text" and value:
        gemini = call_gemini(conv, value)

        # Si Gemini falló, mostrar error detallado + continuar con keywords
        if gemini["_error"]:
            print(f"[gemini] {gemini['_error']}")
            error_msg = _text(f"⚠️ _(Gemini no disponible: {gemini['_error']})_")
            intent = detect_intent(value)
            if intent == "greeting": return [error_msg] + resp_greeting()
            if intent == "help":     return [error_msg] + resp_help(conv)
            if intent == "show_data":return [error_msg] + resp_show_data(conv)
            if intent == "download": return [error_msg] + resp_download(value, conv)
            if intent == "describe": return [error_msg] + resp_describe(value, conv)
            if intent == "start" and conv.step == "welcome":
                conv.process_type = "BUSQUEDA_CONTACTOS"
                conv.step = "ask_empresas"
                return [error_msg, _text("Perfecto, iniciemos la búsqueda 🚀")] + step_messages(conv)
            return [error_msg] + _mid_flow_note(conv)

        # Ejecutar acción de Gemini
        action  = gemini.get("action")
        params  = gemini.get("params") or {}
        msg_txt = gemini.get("message")

        if action == "start_process":
            conv.process_type = "BUSQUEDA_CONTACTOS"
            conv.step = "ask_empresas"
            msgs = [_text(msg_txt)] if msg_txt else [_text("Perfecto, iniciemos la búsqueda 🚀")]
            return msgs + step_messages(conv)

        if action == "download_data":
            pais_g    = params.get("pais") or ""
            empresa_g = params.get("empresa") or ""
            feats = GEOJSON_CACHE.get("features", [])
            filtered = [f for f in feats if
                (not pais_g    or f["properties"].get("pais")    == pais_g) and
                (not empresa_g or f["properties"].get("empresa") == empresa_g)]
            if filtered:
                qs, lp = [], []
                if pais_g:    qs.append(f"pais={pais_g}");       lp.append(pais_g)
                if empresa_g: qs.append(f"empresa={empresa_g}"); lp.append(empresa_g)
                url   = "/api/download-map?" + "&".join(qs) if qs else "/api/download-map"
                label = " — ".join(lp) if lp else "todos"
                msgs = [_text(msg_txt)] if msg_txt else [_text(f"Encontré **{len(filtered)} registros** para {label}.")]
                return msgs + [_link(url, f"⬇️ Descargar {label} ({len(filtered)} registros)")]
            return resp_download(value, conv)

        if action == "show_summary":
            msgs = resp_show_data(conv)
            if msg_txt: msgs.insert(0, _text(msg_txt))
            return msgs

        if action == "filter_map":
            pais_g    = params.get("pais")    or ""
            empresa_g = params.get("empresa") or ""
            return resp_filter_map(pais_g, empresa_g, msg_txt or "")

        if action == "search_map":
            s_query   = params.get("query", "").strip()
            s_field   = params.get("field", "nombre")
            s_empresa = params.get("empresa_filter") or ""
            s_pais    = params.get("pais") or ""
            if s_query:
                msgs = []
                if msg_txt: msgs.append(_text(msg_txt))
                return msgs + resp_search_map(s_query, s_field, s_empresa, s_pais)

        # Gemini respondió con solo texto (action=null)
        if msg_txt:
            return [_text(msg_txt)] + _mid_flow_note(conv)

        # Fallback keywords si Gemini no devolvió nada útil
        intent = detect_intent(value)
        if intent == "greeting": return resp_greeting()
        if intent == "help":     return resp_help(conv)
        if intent == "show_data":return resp_show_data(conv)
        if intent == "download": return resp_download(value, conv)
        if intent == "describe": return resp_describe(value, conv)
        if intent == "start" and conv.step == "welcome":
            conv.process_type = "BUSQUEDA_CONTACTOS"
            conv.step = "ask_empresas"
            return [_text("¡Claro! Iniciemos la búsqueda 🚀")] + step_messages(conv)

    # ---- Pasos del flujo guiado ----

    if conv.step == "ask_empresas":
        if ptype == "file_uploaded" and payload.get("field") == "empresas_file":
            path = payload["path"]
            count = count_csv_rows(path)
            if count == 0:
                return [_text("⚠️ El archivo parece estar vacío. Necesito al menos una empresa en la primera columna."),
                        _upload("empresas_file", "📎 Subir CSV de Empresas", "Primera columna = nombre de empresa")]
            header = _get_csv_header(path)
            if header and header.lower() not in {"empresa", "company", "nombre", "name", "organizations", "organization", "empresas"}:
                conv.pending_confirm_file = {"field": "empresas_file", "path": path, "header": header, "count": count}
                return [
                    _text(f"⚠️ La primera columna del archivo se llama **\"{header}\"**. ¿Es un CSV de **nombres de empresas**?"),
                    _replies([{"label": "✅ Sí, usar este archivo", "value": "CONFIRM_FILE:yes"},
                               {"label": "❌ No, subir otro", "value": "CONFIRM_FILE:no"}]),
                ]
            conv.empresas_path  = path
            conv.empresas_count = count
            conv.advance()
            return [_text(f"✅ {conv.empresas_count} empresas cargadas."),
                    _replies([{"label": "🔄 Reemplazar archivo", "value": "REUPLOAD:empresas_file"}])
                    ] + step_messages(conv)
        if ptype == "text":
            return [_text("Para continuar sube el CSV de empresas 👇"),
                    _upload("empresas_file", "📎 Subir CSV de Empresas", "Primera columna = nombre de empresa")]

    if conv.step == "ask_cargos":
        if ptype == "file_uploaded" and payload.get("field") == "cargos_file":
            path = payload["path"]
            count = count_csv_rows(path)
            if count == 0:
                return [_text("⚠️ El archivo de cargos está vacío."),
                        _upload("cargos_file", "📎 Subir CSV de Cargos")]
            header = _get_csv_header(path)
            if header and header.lower() not in {"cargo", "title", "puesto", "position", "cargos", "job_title", "jobtitle", "role"}:
                conv.pending_confirm_file = {"field": "cargos_file", "path": path, "header": header, "count": count}
                return [
                    _text(f"⚠️ La primera columna del archivo se llama **\"{header}\"**. ¿Es un CSV de **cargos/títulos**?"),
                    _replies([{"label": "✅ Sí, usar este archivo", "value": "CONFIRM_FILE:yes"},
                               {"label": "❌ No, subir otro", "value": "CONFIRM_FILE:no"}]),
                ]
            conv.cargos_path  = path
            conv.cargos_count = count
            conv.advance()
            return [_text(f"✅ {conv.cargos_count} cargos cargados."),
                    _replies([{"label": "🔄 Reemplazar archivo", "value": "REUPLOAD:cargos_file"}])
                    ] + step_messages(conv)
        if ptype == "text":
            return [_text("Sube el CSV de cargos para continuar 👇"),
                    _upload("cargos_file", "📎 Subir CSV de Cargos", "Primera columna = cargo/título")]

    if conv.step == "ask_id_org":
        if ptype == "file_uploaded" and payload.get("field") == "id_org_file":
            path = payload["path"]
            count = count_csv_rows(path)
            if count == 0:
                return [_text("⚠️ El archivo de IDs está vacío."),
                        _upload("id_org_file", "📎 Subir CSV de Id Organizaciones")]
            header = _get_csv_header(path)
            if header and header.lower() not in {"id", "organization_id", "org_id", "apollo_id", "lusha_id", "ids", "organizacion_id"}:
                conv.pending_confirm_file = {"field": "id_org_file", "path": path, "header": header, "count": count}
                return [
                    _text(f"⚠️ La primera columna del archivo se llama **\"{header}\"**. ¿Es un CSV de **IDs de organización**?"),
                    _replies([{"label": "✅ Sí, usar este archivo", "value": "CONFIRM_FILE:yes"},
                               {"label": "❌ No, subir otro", "value": "CONFIRM_FILE:no"}]),
                ]
            conv.id_org_path  = path
            conv.id_org_count = count
            conv.advance()
            return [_text(f"✅ {conv.id_org_count} IDs cargados."),
                    _replies([{"label": "🔄 Reemplazar archivo", "value": "REUPLOAD:id_org_file"}])
                    ] + step_messages(conv)
        if ptype == "text":
            return [_text("Sube el CSV de IDs de organizaciones para continuar 👇"),
                    _upload("id_org_file", "📎 Subir CSV de Id Organizaciones", "Primera columna = ID de organización")]

    if conv.step == "ask_countries":
        if ptype == "countries" and payload.get("paises"):
            conv.paises = payload["paises"]
            conv.paises_names = payload.get("names", conv.paises)
            if not conv.paises:
                return [_text("Selecciona al menos un país."), _countries()]
            conv.advance()
            shown = ", ".join(conv.paises_names[:5])
            extra = len(conv.paises_names) - 5
            label = shown + (f" y {extra} más" if extra > 0 else "")
            map_action = {"type": "map_action", "action": "filter",
                          "pais": conv.paises[0] if len(conv.paises) == 1 else None, "empresa": None}
            return [_text(f"🌎 {label} — confirmado."), map_action] + step_messages(conv)
        if ptype == "text":
            return [_text("Selecciona los países y haz clic en **Confirmar selección** 👇"), _countries()]

    if conv.step == "confirm":
        if ptype == "action" and value == "START":
            result = _launch_job(conv)
            if "error" in result:
                return [_text(f"❌ {result['error']}")]
            conv.job_id = result["job_id"]
            conv.step   = "running"
            return [_text("🚀 Búsqueda iniciada..."), _stream(conv.job_id)]
        if ptype == "action" and value == "RESTART":
            conversations[sid] = ConvState(sid)
            return [_text("↩️ Búsqueda cancelada.")] + resp_greeting()
        return [_text("Revisa el resumen y haz clic en **Iniciar búsqueda** cuando estés listo."),
                _summary(conv.summary_items())]

    if conv.step in ("running", "done"):
        if conv.job_id:
            return [_text("Hay un proceso en curso. Escribe *reiniciar* para cancelar y empezar de nuevo.")]
        return resp_greeting()

    # Fallback
    if ptype == "text" and value:
        return [_text(
            "No estoy seguro de entenderte, pero puedo ayudarte con:\n\n"
            "• **Extraer contactos** — escribe *'quiero buscar contactos'*\n"
            "• **Ver los datos del mapa** — escribe *'qué datos tengo'*\n"
            "• **Descargar registros** — escribe *'descargar Colombia'*\n"
            "• **Conocer los procesos** — escribe *'describe Apollo'*"
        )]

    return resp_greeting() + [_process_menu()]


# ================================================================
# FUNCIONES DE CONSOLIDACIÓN Y VALIDACIÓN
# ================================================================
def _generar_consolidado(output_folder: str, log):
    log(f"\n{'='*60}")
    log("📋 Generando consolidado...")
    fields = ['plataforma','empresa_buscada','organization_id','organization_name',
              'person_id','name','title','country','email','contact_number']
    out = os.path.join(output_folder, "consolidado_depuracion.csv")
    apollo_f = os.path.join(output_folder, "resultados_apollo.csv")
    lusha_f  = os.path.join(output_folder, "resultados_lusha.csv")
    ta = tl = 0
    try:
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            if os.path.isfile(apollo_f):
                with open(apollo_f, "r", encoding="utf-8") as af:
                    for row in csv.DictReader(af):
                        w.writerow({'plataforma':'Apollo','empresa_buscada':row.get('empresa_buscada',''),
                            'organization_id':row.get('organization_id',''),'organization_name':row.get('organization_name',''),
                            'person_id':row.get('id',''),'name':row.get('name',''),'title':row.get('title',''),
                            'country':row.get('country',''),'email':row.get('email',''),'contact_number':row.get('sanitized_number','')})
                        ta += 1
            if os.path.isfile(lusha_f):
                with open(lusha_f, "r", encoding="utf-8") as lf:
                    for row in csv.DictReader(lf):
                        w.writerow({'plataforma':'Lusha','empresa_buscada':row.get('empresa_buscada',''),
                            'organization_id':row.get('companyId',''),'organization_name':row.get('companyName',''),
                            'person_id':row.get('personId',''),'name':row.get('name',''),'title':row.get('jobTitle',''),
                            'country':row.get('pais_buscado',''),'email':'','contact_number':row.get('hasMobilePhone','')})
                        tl += 1
        log(f"   ✅ {ta} Apollo + {tl} Lusha = {ta+tl} total → consolidado_depuracion.csv")
    except Exception as e:
        log(f"❌ Error generando consolidado: {e}")


def _validar_cargos_gemini(output_folder: str, cargos_list: list, stop_event, log):
    import time
    log(f"\n{'='*60}")
    log("🤖 Validando cargos con Gemini...")
    cons = os.path.join(output_folder, "consolidado_depuracion.csv")
    out  = os.path.join(output_folder, "Contactos_ServiLeads.csv")
    if not os.path.isfile(cons):
        log("⚠️ No se encontró consolidado_depuracion.csv, omitiendo validación.")
        return
    registros, fieldnames = [], None
    try:
        with open(cons, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            registros = [dict(r) for r in reader]
    except Exception as e:
        log(f"❌ Error leyendo consolidado: {e}"); return
    if not registros:
        log("⚠️ Consolidado vacío."); return
    if not _GENAI_OK or not GEMINI_API_KEY:
        log("⚠️ Gemini no disponible — copiando consolidado sin filtrar.")
        if not _GENAI_OK:
            log("   ↳ Causa: librería google-generativeai no instalada.")
        if not GEMINI_API_KEY:
            log("   ↳ Causa: GEMINI_API_KEY no configurada en variables de entorno.")
        import shutil; shutil.copy(cons, out); return
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel("gemini-2.5-flash")
    except Exception as e:
        log(f"❌ Error configurando Gemini: {e}"); return
    cargos_txt = ", ".join(cargos_list)
    validos, batch_size = [], 20
    log(f"   📊 {len(registros)} contactos × {len(cargos_list)} cargos de referencia")
    for i in range(0, len(registros), batch_size):
        if stop_event.is_set(): break
        batch = registros[i:i+batch_size]
        titulos = "\n".join(f"{j+1}. {r.get('title','N/A')}" for j,r in enumerate(batch))
        prompt = (f"Cargos de referencia: {cargos_txt}\n\nValida estos títulos:\n{titulos}\n\n"
                  f"Responde SOLO los números VÁLIDOS separados por comas, o NINGUNO.")
        try:
            resp = model.generate_content(prompt).text.strip()
            if resp.upper() != "NINGUNO":
                for num in resp.replace(" ","").split(","):
                    try:
                        idx = int(num)
                        if 1 <= idx <= len(batch): validos.append(batch[idx-1])
                    except ValueError: pass
            log(f"   ✅ Lote {i//batch_size+1}: {len([x for x in resp.split(',') if x.strip().isdigit()])}/{len(batch)} válidos")
        except Exception as e:
            log(f"   ⚠️ Error lote {i//batch_size+1}: {e} — incluyendo lote completo")
            validos.extend(batch)
        time.sleep(1)
    try:
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader(); w.writerows(validos)
        log(f"   📁 Contactos_ServiLeads.csv → {len(validos)} contactos viables de {len(registros)}")
    except Exception as e:
        log(f"❌ Error escribiendo Contactos_ServiLeads.csv: {e}")


# ================================================================
# EJECUTOR DE JOBS
# ================================================================
def _run_job(job_id: str, process_type: str, data: dict):
    job = jobs[job_id]
    log_q: queue.Queue = job["queue"]
    stop_event: threading.Event = job["stop_event"]
    output_dir: str = job["output_dir"]

    def log(msg: str):
        log_q.put(str(msg))

    try:
        if process_type == "BUSQUEDA_CONTACTOS":
            empresas = leer_csv_primera_columna(data["empresas_path"])
            cargos   = leer_csv_primera_columna(data["cargos_path"])
            if not empresas or not cargos:
                log("❌ ERROR: Archivos de empresas o cargos vacíos.")
                return

            # FASE 1: Apollo
            log(f"\n{'='*60}")
            log("📡 FASE 1/2: Buscando contactos en Apollo...")
            log(f"{'='*60}")
            apollo_script.run(APOLLO_API_KEY, empresas, cargos, data["paises"], output_dir, log, stop_event)
            if stop_event.is_set():
                log("🛑 Cancelado durante Apollo."); return

            # Detectar empresas sin resultados en Apollo
            apollo_out = os.path.join(output_dir, "resultados_apollo.csv")
            empresas_con = set()
            if os.path.isfile(apollo_out):
                with open(apollo_out, "r", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        eb = row.get("empresa_buscada", "").strip()
                        if eb: empresas_con.add(eb.lower())
            empresas_sin = [e for e in empresas if e.strip().lower() not in empresas_con]
            log(f"   ✅ Con contactos Apollo: {len(empresas_con)} | ❌ Sin contactos: {len(empresas_sin)}")

            # FASE 2: Lusha solo para empresas sin resultados Apollo
            if empresas_sin and not stop_event.is_set():
                log(f"\n{'='*60}")
                log(f"📡 FASE 2/2: Buscando {len(empresas_sin)} empresa(s) en Lusha...")
                log(f"{'='*60}")
                lusha_script.run(LUSHA_API_KEY, empresas_sin, cargos, data["paises"], output_dir, log, stop_event)
            else:
                log("🎉 Todas las empresas tuvieron resultados en Apollo. Lusha no necesario.")

            if stop_event.is_set():
                log("🛑 Cancelado durante Lusha."); return

            # FASE 3: Consolidado
            _generar_consolidado(output_dir, log)

            # FASE 4: Validación de cargos con Gemini
            if not stop_event.is_set():
                _validar_cargos_gemini(output_dir, cargos, stop_event, log)

            log(f"\n{'='*60}")
            log("🏁 BÚSQUEDA COMPLETADA")
            log(f"   📁 Contactos_ServiLeads.csv → resultado final validado")
            log(f"{'='*60}\n")

        else:
            log(f"⚠️ Proceso desconocido: {process_type}")

    except Exception as exc:
        import traceback
        log(f"❌ ERROR FATAL: {exc}")
        log(traceback.format_exc())
    finally:
        job["done"] = True
        log_q.put(None)  # sentinel → cierra el stream


def _launch_job(conv: ConvState) -> dict:
    connector = next((c for c in CONNECTORS if c["id"] == conv.process_type), None)
    if not connector:
        return {"error": "Tipo de proceso no reconocido"}
    if not APOLLO_API_KEY and not LUSHA_API_KEY:
        return {"error": "APOLLO_API_KEY y LUSHA_API_KEY no configuradas en el servidor"}

    job_id = str(uuid.uuid4())
    output_dir = os.path.join(conv.upload_dir, "output")
    os.makedirs(output_dir, exist_ok=True)

    log_q = queue.Queue()
    stop_event = threading.Event()
    jobs[job_id] = {
        "queue": log_q,
        "stop_event": stop_event,
        "output_dir": output_dir,
        "output_filename": connector["output_filename"],
        "done": False,
        "process_type": conv.process_type,
    }

    data = {
        "paises":        conv.paises,
        "empresas_path": conv.empresas_path,
        "cargos_path":   conv.cargos_path,
    }

    t = threading.Thread(target=_run_job, args=(job_id, conv.process_type, data), daemon=True)
    t.start()
    return {"job_id": job_id}


# ================================================================
# RUTAS
# ================================================================

@app.route("/login")
def login():
    if flask_session.get("user", {}).get("company"):
        return redirect(url_for("index"))
    return render_template("login.html", has_google=bool(GOOGLE_CLIENT_ID))


def _build_redirect_uri() -> str:
    """Siempre devuelve HTTPS — Google rechaza http:// en producción."""
    if GOOGLE_REDIRECT_URI:
        return GOOGLE_REDIRECT_URI
    base = request.url_root.rstrip("/")
    base = base.replace("http://", "https://", 1)
    return base + "/auth/callback"


@app.route("/debug/oauth")
def debug_oauth():
    """Ruta de diagnóstico — muestra la URI exacta que se enviará a Google."""
    uri = _build_redirect_uri()
    return jsonify({
        "redirect_uri":        uri,
        "GOOGLE_CLIENT_ID_set": bool(GOOGLE_CLIENT_ID),
        "GOOGLE_REDIRECT_URI_env": GOOGLE_REDIRECT_URI or "(no configurado, se auto-calcula)",
        "request_url_root":    request.url_root,
    })


@app.route("/auth/google")
def auth_google():
    if not GOOGLE_CLIENT_ID:
        return "GOOGLE_CLIENT_ID no configurado en Render → Environment Variables", 500
    redirect_uri = _build_redirect_uri()
    flask_session["oauth_state"]        = secrets.token_urlsafe(32)
    flask_session["oauth_redirect_uri"] = redirect_uri
    params = {
        "client_id":     GOOGLE_CLIENT_ID,
        "redirect_uri":  redirect_uri,
        "scope":         "openid email profile https://www.googleapis.com/auth/gmail.send",
        "response_type": "code",
        "state":         flask_session["oauth_state"],
        "access_type":   "offline",
        "prompt":        "consent select_account",
    }
    return redirect(GOOGLE_AUTH_URL + "?" + urlencode(params))


@app.route("/auth/callback")
def auth_callback():
    code     = request.args.get("code")
    state    = request.args.get("state")
    expected = flask_session.pop("oauth_state", None)
    redirect_uri = flask_session.pop("oauth_redirect_uri", "")

    if not code or state != expected:
        return redirect(url_for("login"))

    token_resp = requests.post(GOOGLE_TOKEN_URL, data={
        "code":          code,
        "client_id":     GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uri":  redirect_uri,
        "grant_type":    "authorization_code",
    })
    token_data   = token_resp.json()
    access_token = token_data.get("access_token")
    if not access_token:
        return redirect(url_for("login"))

    info = requests.get(GOOGLE_INFO_URL,
                        headers={"Authorization": f"Bearer {access_token}"}).json()

    email  = info.get("email", "").lower().strip()
    domain = email.split("@")[-1] if "@" in email else ""

    # Listas de acceso (vacías = sin restricción)
    _emails  = [e.strip().lower() for e in os.environ.get("ALLOWED_EMAILS",  "").split(",") if e.strip()]
    _domains = [d.strip().lower() for d in os.environ.get("ALLOWED_DOMAINS", "").split(",") if d.strip()]

    if (_emails or _domains):
        allowed = (email in _emails) or (domain in _domains)
        if not allowed:
            return render_template("login.html", has_google=bool(GOOGLE_CLIENT_ID),
                                   access_error=f"La cuenta {email} no tiene acceso a esta plataforma.")

    flask_session["user"] = {
        "email":     info.get("email", ""),
        "name":      info.get("name", ""),
        "picture":   info.get("picture", ""),
        "google_id": info.get("sub", ""),
        "company":   None,
    }
    flask_session["gmail_token"] = {
        "access_token":  access_token,
        "refresh_token": token_data.get("refresh_token"),
    }
    return redirect(url_for("profile"))


@app.route("/profile", methods=["GET", "POST"])
def profile():
    if "user" not in flask_session:
        return redirect(url_for("login"))
    user = flask_session["user"]
    if request.method == "POST":
        name    = request.form.get("name", "").strip()
        company = request.form.get("company", "").strip()
        if not name or company not in ALLOWED_COMPANIES:
            return render_template("profile.html", user=user,
                                   companies=ALLOWED_COMPANIES,
                                   error="Completa todos los campos.")
        flask_session["user"] = {**user, "name": name, "company": company}
        flask_session.modified = True
        return redirect(url_for("index"))
    return render_template("profile.html", user=user, companies=ALLOWED_COMPANIES)


@app.route("/logout")
def logout():
    flask_session.clear()
    return redirect(url_for("login"))


@app.route("/")
@login_required
def index():
    return render_template("map.html", user=flask_session.get("user", {}))


@app.route("/api/geojson")
def geojson():
    """Devuelve todos los puntos del shapefile como GeoJSON."""
    return jsonify(GEOJSON_CACHE)


@app.route("/api/download-map")
def download_map():
    """Descarga los puntos del shapefile filtrados por pais y/o empresa como CSV."""
    import io as _io
    pais    = request.args.get("pais",    "").strip()
    empresa = request.args.get("empresa", "").strip()
    feats   = GEOJSON_CACHE.get("features", [])
    filtered = [
        f for f in feats
        if (not pais    or _norm(pais)    in _norm(f["properties"].get("pais",    "")))
        and (not empresa or _norm(empresa) in _norm(f["properties"].get("empresa", "")))
    ]
    if not filtered:
        return jsonify({"error": "No hay registros para esos filtros"}), 404

    fields  = ["pais", "empresa", "nombre", "cargo", "correo", "telefono", "url"]
    output  = _io.StringIO()
    writer  = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for f in filtered:
        writer.writerow(f["properties"])
    output.seek(0)

    slug = (pais or empresa or "todos").replace(" ", "_")
    from flask import make_response as _mkr
    resp = _mkr(output.getvalue().encode("utf-8"))
    resp.headers["Content-Type"]        = "text/csv; charset=utf-8"
    resp.headers["Content-Disposition"] = f'attachment; filename="servi_leads_{slug}.csv"'
    return resp


@app.route("/api/filters")
def filters():
    """Devuelve los valores únicos de país y empresa para los filtros del mapa."""
    feats = GEOJSON_CACHE.get("features", [])
    paises   = sorted(set(f["properties"]["pais"]    for f in feats if f["properties"].get("pais")))
    empresas = sorted(set(f["properties"]["empresa"] for f in feats if f["properties"].get("empresa")))
    return jsonify({"paises": paises, "empresas": empresas})


@app.route("/api/chat", methods=["POST"])
def chat():
    """Recibe un turno del usuario y devuelve mensajes del agente."""
    body = request.get_json(force=True, silent=True) or {}
    sid = body.get("sid") or request.headers.get("X-Session-Id", str(uuid.uuid4()))
    try:
        messages = handle_turn(sid, body)
    except Exception as exc:
        import traceback
        print(f"[chat] error en handle_turn: {exc}\n{traceback.format_exc()}")
        messages = [{"type": "text", "content": f"⚠️ Error interno: {exc}"}]
    return jsonify({"sid": sid, "messages": messages})


@app.route("/api/upload/<sid>/<field>", methods=["POST"])
def upload_file(sid: str, field: str):
    """Guarda un CSV subido y notifica al motor conversacional."""
    VALID_FIELDS = {"empresas_file", "cargos_file", "id_org_file"}
    if field not in VALID_FIELDS:
        return jsonify({"error": "Campo no válido"}), 400

    uploaded = request.files.get("file")
    if not uploaded or not uploaded.filename:
        return jsonify({"error": "No se recibió archivo"}), 400
    if not uploaded.filename.lower().endswith(".csv"):
        return jsonify({"error": "Solo se aceptan archivos .csv"}), 400

    conv = conversations.get(sid)
    if not conv:
        return jsonify({"error": "Sesión no encontrada"}), 404

    save_path = os.path.join(conv.upload_dir, f"{field}.csv")
    uploaded.save(save_path)

    # Procesar el turno como si el usuario "entregó" el archivo
    payload = {"type": "file_uploaded", "field": field, "path": save_path}
    messages = handle_turn(sid, payload)
    return jsonify({"sid": sid, "messages": messages})


@app.route("/api/stream/<job_id>")
def stream(job_id: str):
    """SSE: transmite logs en tiempo real."""
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job no encontrado"}), 404

    def generate():
        yield "retry: 3600000\n\n"  # desactiva reconexión automática del navegador
        q: queue.Queue = job["queue"]
        while True:
            try:
                msg = q.get(timeout=25)
                if msg is None:
                    yield "data: __DONE__\n\n"
                    break
                safe = str(msg).replace("\r", "").replace("\n", "\\n")
                yield f"data: {safe}\n\n"
            except queue.Empty:
                yield "data: __PING__\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/cancel/<job_id>", methods=["POST"])
def cancel(job_id: str):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job no encontrado"}), 404
    job["stop_event"].set()
    return jsonify({"status": "cancelado"})


@app.route("/api/download/<job_id>")
def download(job_id: str):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job no encontrado"}), 404
    output_path = os.path.join(job["output_dir"], job["output_filename"])
    if not os.path.exists(output_path):
        return jsonify({"error": "Archivo no generado. Es posible que no se encontraron resultados."}), 404
    return send_file(output_path, as_attachment=True, download_name=job["output_filename"])


GMAIL_SEND_URL = "https://gmail.googleapis.com/gmail/v1/users/me/messages/send"
GMAIL_TOKEN_REFRESH_URL = "https://oauth2.googleapis.com/token"

PLANTILLA_EMAIL = """\
<!DOCTYPE html>
<html lang="es">
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background-color:#f4f4f4;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f4f4;padding:20px;">
    <tr><td align="center">
      <table width="600" cellpadding="0" cellspacing="0"
             style="background-color:#ffffff;border-radius:8px;overflow:hidden;">
        <tr>
          <td style="background-color:#1a73e8;color:#ffffff;padding:30px;text-align:center;">
            <h1 style="margin:0;font-size:22px;">Servinformación</h1>
            <p style="margin:5px 0 0;font-size:13px;opacity:.85;">Partner Premier de Google</p>
          </td>
        </tr>
        <tr>
          <td style="padding:30px;color:#333333;line-height:1.8;font-size:15px;">
            <p>Hola, buenos días.</p>
            <p>Mi nombre es <strong>{nombre_emisor}</strong> y formo parte de
               <strong>Servinformación</strong>, partner premier de Google.</p>
            <p>Me contacto con el señor(a) <strong>{nombre_contacto}</strong>,
               <strong>{cargo_contacto}</strong> de la empresa
               <strong>{empresa}</strong>.</p>
            <p>Queremos compartir con usted algunas soluciones de
               <strong>transformación digital</strong> que pueden generar un gran
               impacto en su empresa y queremos verificar sus datos de contacto
               para que uno de nuestros especialistas se ponga en contacto con
               usted y le cuente detalladamente cómo podemos apoyar en:</p>
            <ul style="padding-left:20px;">
              <li>Optimización de procesos</li>
              <li>Mejora en la toma de decisiones</li>
              <li>Potenciar su crecimiento mediante herramientas tecnológicas avanzadas</li>
            </ul>
            <p>Quedamos atentos a su respuesta.</p>
            <p style="margin-top:25px;">Cordialmente,<br>
               <strong>{nombre_emisor}</strong><br>
               Servinformación — Partner Premier de Google</p>
          </td>
        </tr>
        <tr>
          <td style="background-color:#ecf0f1;padding:20px;text-align:center;
                      color:#888888;font-size:12px;">
            <p style="margin:0;">&copy; 2026 Servinformación. Todos los derechos reservados.</p>
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""


def _gmail_refresh(refresh_token: str) -> str | None:
    """Renueva el access_token usando el refresh_token. Devuelve el nuevo token o None."""
    r = requests.post(GMAIL_TOKEN_REFRESH_URL, data={
        "client_id":     GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "refresh_token": refresh_token,
        "grant_type":    "refresh_token",
    })
    return r.json().get("access_token") if r.status_code == 200 else None


@app.route("/api/send_email", methods=["POST"])
@login_required
def send_email():
    import base64
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    data = request.get_json(force=True)
    destinatario   = (data.get("destinatario") or "").strip()
    nombre_contacto = (data.get("nombre_contacto") or "").strip()
    cargo_contacto  = (data.get("cargo_contacto") or "").strip()
    empresa         = (data.get("empresa") or "").strip()

    if not destinatario:
        return jsonify({"error": "Destinatario requerido"}), 400

    user   = flask_session["user"]
    nombre_emisor = user.get("name") or user.get("email", "")

    # Obtener token de Gmail desde la sesión
    gmail_token = flask_session.get("gmail_token", {})
    access_token = gmail_token.get("access_token")
    refresh_token = gmail_token.get("refresh_token")

    if not access_token:
        return jsonify({"error": "Sin autorización de Gmail. Cierra sesión y vuelve a entrar."}), 401

    # Construir el mensaje
    msg = MIMEMultipart("alternative")
    msg["Subject"] = "Soluciones de Transformación Digital — Servinformación"
    msg["To"] = destinatario
    html = PLANTILLA_EMAIL.format(
        nombre_emisor=nombre_emisor,
        nombre_contacto=nombre_contacto or "contacto",
        cargo_contacto=cargo_contacto or "",
        empresa=empresa or "",
    )
    msg.attach(MIMEText(html, "html"))
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()

    def _send(token):
        return requests.post(
            GMAIL_SEND_URL,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"raw": raw},
        )

    resp = _send(access_token)

    # Si expiró el token, intenta refrescar
    if resp.status_code == 401 and refresh_token:
        new_token = _gmail_refresh(refresh_token)
        if new_token:
            flask_session["gmail_token"]["access_token"] = new_token
            flask_session.modified = True
            resp = _send(new_token)

    if resp.status_code in (200, 204):
        return jsonify({"ok": True})
    return jsonify({"error": f"Gmail error {resp.status_code}: {resp.text[:200]}"}), 502


# ================================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)

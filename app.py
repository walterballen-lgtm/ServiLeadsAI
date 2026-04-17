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

import os, uuid, queue, threading, tempfile, csv
from flask import (
    Flask, render_template, request, jsonify,
    Response, send_file, stream_with_context,
)

import apollo_script, lusha_script, apollo_org, lusha_org

try:
    import shapefile as pyshp
    _SHP_OK = True
except ImportError:
    _SHP_OK = False

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "servi-leads-ai-2024")
app.config["JSON_AS_ASCII"] = False  # UTF-8 limpio en respuestas JSON

# ================================================================
# API KEYS — configúralas en Render → Environment Variables
# (nunca las pongas directamente en el código)
# ================================================================
APOLLO_API_KEY = os.environ.get("APOLLO_API_KEY", "")
LUSHA_API_KEY  = os.environ.get("LUSHA_API_KEY",  "")


# ================================================================
# CONECTORES — edita aquí para agregar botones / nuevas APIs
# ================================================================
CONNECTORS = [
    {
        "id": "APOLLO_CONTACT",
        "label": "Apollo — Contactos",
        "emoji": "🔍",
        "color": "#867903",
        "required_api": "apollo_api",
        "required_files": ["empresas_file", "cargos_file"],
        "needs_countries": True,
        "output_filename": "resultados_apollo.csv",
    },
    {
        "id": "APOLLO_ORG",
        "label": "Apollo — Organizaciones",
        "emoji": "🏢",
        "color": "#867903",
        "required_api": "apollo_api",
        "required_files": ["id_org_file"],
        "needs_countries": False,
        "output_filename": "apollo_organizations_output.csv",
    },
    {
        "id": "LUSHA_CONTACT",
        "label": "Lusha — Contactos",
        "emoji": "🔍",
        "color": "#53045F",
        "required_api": "lusha_api",
        "required_files": ["empresas_file", "cargos_file"],
        "needs_countries": True,
        "output_filename": "resultados_lusha.csv",
    },
    {
        "id": "LUSHA_ORG",
        "label": "Lusha — Organizaciones",
        "emoji": "🏢",
        "color": "#53045F",
        "required_api": "lusha_api",
        "required_files": ["id_org_file"],
        "needs_countries": False,
        "output_filename": "lusha_organizations_output.csv",
    },
    # --- Ejemplo: nuevo conector ---
    # {
    #     "id": "SIGNALHIRE_CONTACT",
    #     "label": "SignalHire — Contactos",
    #     "emoji": "📡",
    #     "color": "#083588",
    #     "required_api": "signalhire_api",
    #     "required_files": ["empresas_file", "cargos_file"],
    #     "needs_countries": True,
    #     "output_filename": "resultados_signalhire.csv",
    # },
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
    sf = pyshp.Reader(path, encoding="cp1252")
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

    # Secuencia de pasos por tipo de proceso
    STEP_MAP = {
        "APOLLO_CONTACT": ["ask_empresas", "ask_cargos", "ask_countries", "confirm"],
        "APOLLO_ORG":     ["ask_id_org", "confirm"],
        "LUSHA_CONTACT":  ["ask_empresas", "ask_cargos", "ask_countries", "confirm"],
        "LUSHA_ORG":      ["ask_id_org", "confirm"],
        # Agrega nuevos procesos aquí
    }

    def __init__(self, sid: str):
        self.sid = sid
        self.step = "welcome"
        self.process_type: str = None
        self.apollo_api: str = None
        self.lusha_api: str = None
        self.paises: list = []
        self.paises_names: list = []
        self.empresas_path: str = None
        self.cargos_path: str = None
        self.id_org_path: str = None
        self.empresas_count: int = 0
        self.cargos_count: int = 0
        self.id_org_count: int = 0
        self.job_id: str = None
        self.upload_dir = tempfile.mkdtemp(prefix=f"conv_{sid[:8]}_")

    @property
    def api_name(self) -> str:
        if not self.process_type:
            return ""
        return "Apollo" if "APOLLO" in self.process_type else "Lusha"

    @property
    def api_key(self) -> str:
        return self.apollo_api if "APOLLO" in (self.process_type or "") else self.lusha_api

    def advance(self):
        steps = self.STEP_MAP.get(self.process_type, [])
        if self.step in steps:
            idx = steps.index(self.step)
            if idx + 1 < len(steps):
                self.step = steps[idx + 1]

    def summary_items(self) -> list:
        items = [
            ("Tipo de búsqueda", self.process_type.replace("_", " ").title() if self.process_type else "—"),
            ("API", f"{self.api_name} ✓ configurada"),
        ]
        if self.empresas_path:
            items.append(("Empresas", f"{self.empresas_count} registros"))
        if self.cargos_path:
            items.append(("Cargos", f"{self.cargos_count} registros"))
        if self.id_org_path:
            items.append(("Id Organizaciones", f"{self.id_org_count} IDs"))
        if self.paises_names:
            shown = self.paises_names[:4]
            extra = len(self.paises_names) - 4
            label = ", ".join(shown) + (f" +{extra} más" if extra > 0 else "")
            items.append(("Países", label))
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
# MENSAJES POR PASO
# ================================================================
def welcome_messages() -> list:
    options = [
        {"label": f"{c['emoji']} {c['label']}", "value": c["id"]}
        for c in CONNECTORS
    ]
    return [
        _text("¡Hola! 👋 Soy **ServiLeads AI**, tu asistente de extracción de datos.\n\nTe guiaré paso a paso para configurar tu búsqueda. ¿Qué tipo de búsqueda quieres realizar?"),
        _replies(options),
    ]


def step_messages(conv: ConvState) -> list:
    step = conv.step

    if step == "ask_api":
        return [_text(
            f"Perfecto, vamos a buscar en **{conv.process_type.replace('_', ' ').title()}**.\n\n"
            f"🔑 ¿Cuál es tu **API Key de {conv.api_name}**?\n\n"
            f"_(Escríbela directamente en el chat — la mantendremos segura)_"
        )]

    if step == "ask_empresas":
        return [
            _text("📁 Ahora necesito el **CSV de Empresas**.\n\nEl archivo debe tener en la primera columna el nombre de cada empresa a buscar."),
            _upload("empresas_file", "📎 Subir CSV de Empresas", "Primera columna = nombre de empresa"),
        ]

    if step == "ask_cargos":
        return [
            _text("📁 Ahora el **CSV de Cargos**.\n\nPrimera columna = título del cargo (ej: CEO, Director Comercial, Gerente)."),
            _upload("cargos_file", "📎 Subir CSV de Cargos", "Primera columna = cargo/título"),
        ]

    if step == "ask_id_org":
        return [
            _text("📁 Necesito el **CSV de Id Organizaciones**.\n\nPrimera columna = ID de la organización en la plataforma."),
            _upload("id_org_file", "📎 Subir CSV de Id Organizaciones", "Primera columna = ID de organización"),
        ]

    if step == "ask_countries":
        return [
            _text("🌎 ¿En qué **países** quieres buscar?\n\nSelecciona uno o más y haz clic en **Confirmar selección**."),
            _countries(),
        ]

    if step == "confirm":
        return [
            _text("✅ ¡Listo! Tengo todo lo necesario. Aquí está el resumen de tu búsqueda:"),
            _summary(conv.summary_items()),
        ]

    return [_text("Estado desconocido. Escribe *reiniciar* para empezar de nuevo.")]


# ================================================================
# MOTOR DE CONVERSACIÓN
# ================================================================
RESET_WORDS = {"reiniciar", "restart", "reset", "inicio", "hola", "empezar", "start"}


def handle_turn(sid: str, payload: dict) -> list:
    """
    Procesa un turno del usuario y devuelve la lista de mensajes del agente.

    payload keys:
      type: "text" | "action" | "file_uploaded" | "countries"
      value: contenido según el tipo
      field: (solo en file_uploaded) nombre del campo
      path: (solo en file_uploaded) ruta temporal del archivo
      paises: (solo en countries) lista de valores en inglés
      names: (solo en countries) lista de nombres en español
    """
    conv = conversations.get(sid)
    if conv is None:
        conv = ConvState(sid)
        conversations[sid] = conv

    ptype = payload.get("type", "text")
    value = str(payload.get("value", "")).strip()

    # Reinicio en cualquier momento
    if ptype == "text" and value.lower() in RESET_WORDS:
        conversations[sid] = ConvState(sid)
        return welcome_messages()

    # ---------- INIT / WELCOME ----------
    if ptype == "init":
        conv.step = "welcome"
        return welcome_messages()

    if conv.step == "welcome":
        if ptype == "action" and value in {c["id"] for c in CONNECTORS}:
            conv.process_type = value
            conv.step = conv.STEP_MAP[value][0]
            return step_messages(conv)
        return welcome_messages()

    # ---------- CSV EMPRESAS ----------
    if conv.step == "ask_empresas":
        if ptype == "file_uploaded" and payload.get("field") == "empresas_file":
            conv.empresas_path = payload["path"]
            conv.empresas_count = count_csv_rows(conv.empresas_path)
            if conv.empresas_count == 0:
                return [_text("⚠️ El archivo parece estar vacío. Sube un CSV con al menos una empresa en la primera columna."),
                        _upload("empresas_file", "📎 Subir CSV de Empresas", "Primera columna = nombre de empresa")]
            conv.advance()
            return [_text(f"✅ CSV de empresas cargado — **{conv.empresas_count}** registros.")] + step_messages(conv)
        return [_text("Por favor sube el archivo CSV de empresas usando el botón de arriba. 👆")]

    # ---------- CSV CARGOS ----------
    if conv.step == "ask_cargos":
        if ptype == "file_uploaded" and payload.get("field") == "cargos_file":
            conv.cargos_path = payload["path"]
            conv.cargos_count = count_csv_rows(conv.cargos_path)
            if conv.cargos_count == 0:
                return [_text("⚠️ El archivo de cargos está vacío. Asegúrate de que la primera columna tenga los títulos."),
                        _upload("cargos_file", "📎 Subir CSV de Cargos")]
            conv.advance()
            return [_text(f"✅ CSV de cargos cargado — **{conv.cargos_count}** registros.")] + step_messages(conv)
        return [_text("Por favor sube el archivo CSV de cargos. 👆")]

    # ---------- CSV ID ORG ----------
    if conv.step == "ask_id_org":
        if ptype == "file_uploaded" and payload.get("field") == "id_org_file":
            conv.id_org_path = payload["path"]
            conv.id_org_count = count_csv_rows(conv.id_org_path)
            if conv.id_org_count == 0:
                return [_text("⚠️ El archivo de IDs está vacío."),
                        _upload("id_org_file", "📎 Subir CSV de Id Organizaciones")]
            conv.advance()
            return [_text(f"✅ CSV de IDs cargado — **{conv.id_org_count}** registros.")] + step_messages(conv)
        return [_text("Por favor sube el archivo CSV de IDs de organizaciones. 👆")]

    # ---------- PAÍSES ----------
    if conv.step == "ask_countries":
        if ptype == "countries" and payload.get("paises"):
            conv.paises = payload["paises"]
            conv.paises_names = payload.get("names", conv.paises)
            if not conv.paises:
                return [_text("Selecciona al menos un país. 👆"), _countries()]
            conv.advance()
            shown = ", ".join(conv.paises_names[:5])
            extra = len(conv.paises_names) - 5
            label = shown + (f" y {extra} más" if extra > 0 else "")
            # Emitir acción de mapa: filtra/vuela al país si solo hay uno seleccionado
            map_action = {
                "type": "map_action",
                "action": "filter",
                "pais": conv.paises[0] if len(conv.paises) == 1 else None,
                "empresa": None,
            }
            return [_text(f"🌎 Países confirmados: **{label}**"), map_action] + step_messages(conv)
        return [_text("Selecciona los países y haz clic en **Confirmar selección**. 👆"), _countries()]

    # ---------- CONFIRMAR ----------
    if conv.step == "confirm":
        if ptype == "action" and value == "START":
            result = _launch_job(conv)
            if "error" in result:
                return [_text(f"❌ {result['error']}")]
            conv.job_id = result["job_id"]
            conv.step = "running"
            return [
                _text("🚀 ¡Búsqueda iniciada! Los logs aparecerán aquí en tiempo real..."),
                _stream(conv.job_id),
            ]
        if ptype == "action" and value == "RESTART":
            conversations[sid] = ConvState(sid)
            return welcome_messages()
        return [
            _text("Haz clic en **Iniciar búsqueda** para comenzar."),
            _summary(conv.summary_items()),
        ]

    # ---------- RUNNING / DONE ----------
    if conv.step in ("running", "done"):
        if conv.job_id:
            return [_text("Ya hay un proceso en curso. Espera a que termine o escribe *reiniciar* para cancelar y empezar de nuevo.")]
        return welcome_messages()

    return [_text("No entendí eso. 😊 Escribe *reiniciar* para volver al inicio."), *welcome_messages()]


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
        if process_type == "APOLLO_CONTACT":
            empresas = leer_csv_primera_columna(data["empresas_path"])
            cargos   = leer_csv_primera_columna(data["cargos_path"])
            if not empresas or not cargos:
                log("❌ ERROR: Archivos de empresas o cargos vacíos.")
                return
            apollo_script.run(data["apollo_api"], empresas, cargos, data["paises"], output_dir, log, stop_event)

        elif process_type == "APOLLO_ORG":
            apollo_org.run(data["apollo_api"], data["id_org_path"], output_dir, log, stop_event)

        elif process_type == "LUSHA_CONTACT":
            empresas = leer_csv_primera_columna(data["empresas_path"])
            cargos   = leer_csv_primera_columna(data["cargos_path"])
            if not empresas or not cargos:
                log("❌ ERROR: Archivos de empresas o cargos vacíos.")
                return
            lusha_script.run(data["lusha_api"], empresas, cargos, data["paises"], output_dir, log, stop_event)

        elif process_type == "LUSHA_ORG":
            lusha_org.run(data["lusha_api"], data["id_org_path"], output_dir, log, stop_event)

        # --- Agrega nuevos elif aquí ---

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

    needed_key = APOLLO_API_KEY if "APOLLO" in conv.process_type else LUSHA_API_KEY
    if not needed_key:
        api_name = "APOLLO_API_KEY" if "APOLLO" in conv.process_type else "LUSHA_API_KEY"
        return {"error": f"Variable de entorno {api_name} no configurada en el servidor"}

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
        "apollo_api":   APOLLO_API_KEY,
        "lusha_api":    LUSHA_API_KEY,
        "paises":       conv.paises,
        "empresas_path": conv.empresas_path,
        "cargos_path":   conv.cargos_path,
        "id_org_path":   conv.id_org_path,
    }

    t = threading.Thread(target=_run_job, args=(job_id, conv.process_type, data), daemon=True)
    t.start()
    return {"job_id": job_id}


# ================================================================
# RUTAS
# ================================================================

@app.route("/")
def index():
    return render_template("map.html")


@app.route("/api/geojson")
def geojson():
    """Devuelve todos los puntos del shapefile como GeoJSON."""
    return jsonify(GEOJSON_CACHE)


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
    messages = handle_turn(sid, body)
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


# ================================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)

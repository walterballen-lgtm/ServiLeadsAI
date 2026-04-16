"""
app.py - Servidor Web Flask para Extractor de Datos v3.8

=============================================================
PARA AGREGAR UN NUEVO BOTÓN/CONECTOR DE API:
  1. Agrega un dict a la lista CONNECTORS (sección de abajo)
  2. Agrega un case en la función _run_job()
  3. ¡Listo! El botón aparece automáticamente en la UI
=============================================================
"""

import os
import uuid
import queue
import threading
import tempfile
import csv

from flask import Flask, render_template, request, jsonify, Response, send_file, stream_with_context

import apollo_script
import lusha_script
import apollo_org
import lusha_org

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024  # 10 MB máximo por upload


# ================================================================
# CONECTORES — EDITA AQUÍ PARA AGREGAR/QUITAR BOTONES
# Cada dict genera un botón en la UI automáticamente.
# ================================================================
CONNECTORS = [
    {
        "id": "APOLLO_CONTACT",
        "label": "Apollo Contactos",
        "color": "#867903",
        "hover_color": "#E0CC11",
        "required_api": "apollo_api",           # qué campo de API key se necesita
        "required_files": ["empresas_file", "cargos_file"],  # archivos CSV requeridos
        "needs_countries": True,                # ¿necesita selección de países?
        "output_filename": "resultados_apollo.csv",
    },
    {
        "id": "APOLLO_ORG",
        "label": "Apollo Organizaciones",
        "color": "#867903",
        "hover_color": "#E0CC11",
        "required_api": "apollo_api",
        "required_files": ["id_org_file"],
        "needs_countries": False,
        "output_filename": "apollo_organizations_output.csv",
    },
    {
        "id": "LUSHA_CONTACT",
        "label": "Lusha Contactos",
        "color": "#53045F",
        "hover_color": "#9E06B6",
        "required_api": "lusha_api",
        "required_files": ["empresas_file", "cargos_file"],
        "needs_countries": True,
        "output_filename": "resultados_lusha.csv",
    },
    {
        "id": "LUSHA_ORG",
        "label": "Lusha Organizaciones",
        "color": "#53045F",
        "hover_color": "#9E06B6",
        "required_api": "lusha_api",
        "required_files": ["id_org_file"],
        "needs_countries": False,
        "output_filename": "lusha_organizations_output.csv",
    },
    # --- EJEMPLO: cómo agregar un nuevo conector (descomenta y adapta) ---
    # {
    #     "id": "SIGNALHIRE_CONTACT",
    #     "label": "SignalHire Contactos",
    #     "color": "#083588",
    #     "hover_color": "#0A46B6",
    #     "required_api": "signalhire_api",
    #     "required_files": ["empresas_file", "cargos_file"],
    #     "needs_countries": True,
    #     "output_filename": "resultados_signalhire.csv",
    # },
]


# ================================================================
# PAÍSES — Mapeo Español (UI) → Inglés (APIs)
# ================================================================
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
# Store de jobs en memoria (job_id → datos del job)
# ================================================================
jobs: dict = {}


# ================================================================
# HELPERS
# ================================================================

def leer_csv_primera_columna(filepath: str) -> list:
    """Lee la primera columna de un CSV (omite encabezado). Soporta latin-1 y utf-8."""
    result = []
    encodings = ["latin-1", "utf-8-sig", "utf-8"]
    for enc in encodings:
        try:
            with open(filepath, mode="r", encoding=enc) as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if row and row[0].strip():
                        result.append(row[0].strip()[:500])
            return result
        except UnicodeDecodeError:
            continue
    return result


# ================================================================
# EJECUTOR DE JOBS — agrega aquí el case para nuevos conectores
# ================================================================

def _run_job(job_id: str, process_type: str, data: dict):
    """Corre el proceso en un hilo background y envía logs a la queue del job."""
    job = jobs[job_id]
    log_q: queue.Queue = job["queue"]
    stop_event: threading.Event = job["stop_event"]
    output_dir: str = job["output_dir"]

    def log(msg: str):
        log_q.put(str(msg))

    try:
        # ----------------------------------------------------------
        # APOLLO — Contactos
        # ----------------------------------------------------------
        if process_type == "APOLLO_CONTACT":
            empresas = leer_csv_primera_columna(data["empresas_path"])
            cargos = leer_csv_primera_columna(data["cargos_path"])
            if not empresas or not cargos:
                log("❌ ERROR: Los archivos de empresas o cargos están vacíos.")
                return
            apollo_script.run(
                data["apollo_api"], empresas, cargos,
                data["paises"], output_dir, log, stop_event,
            )

        # ----------------------------------------------------------
        # APOLLO — Organizaciones
        # ----------------------------------------------------------
        elif process_type == "APOLLO_ORG":
            apollo_org.run(
                data["apollo_api"], data["id_org_path"],
                output_dir, log, stop_event,
            )

        # ----------------------------------------------------------
        # LUSHA — Contactos
        # ----------------------------------------------------------
        elif process_type == "LUSHA_CONTACT":
            empresas = leer_csv_primera_columna(data["empresas_path"])
            cargos = leer_csv_primera_columna(data["cargos_path"])
            if not empresas or not cargos:
                log("❌ ERROR: Los archivos de empresas o cargos están vacíos.")
                return
            lusha_script.run(
                data["lusha_api"], empresas, cargos,
                data["paises"], output_dir, log, stop_event,
            )

        # ----------------------------------------------------------
        # LUSHA — Organizaciones
        # ----------------------------------------------------------
        elif process_type == "LUSHA_ORG":
            lusha_org.run(
                data["lusha_api"], data["id_org_path"],
                output_dir, log, stop_event,
            )

        # ----------------------------------------------------------
        # NUEVO CONECTOR — agrega un elif aquí
        # elif process_type == "SIGNALHIRE_CONTACT":
        #     import signalhire_script
        #     signalhire_script.run(data["signalhire_api"], ...)
        # ----------------------------------------------------------

        else:
            log(f"⚠️ Tipo de proceso desconocido: {process_type}")

    except Exception as exc:
        import traceback
        log(f"❌ ERROR FATAL: {exc}")
        log(traceback.format_exc())
    finally:
        job["done"] = True
        log_q.put(None)  # Sentinel → indica fin del stream


# ================================================================
# RUTAS
# ================================================================

@app.route("/")
def index():
    return render_template("index.html", connectors=CONNECTORS, paises=PAISES_MAPEO)


@app.route("/api/start", methods=["POST"])
def start():
    process_type = request.form.get("process_type")
    if not process_type:
        return jsonify({"error": "process_type requerido"}), 400

    connector = next((c for c in CONNECTORS if c["id"] == process_type), None)
    if not connector:
        return jsonify({"error": f"Conector '{process_type}' no encontrado"}), 400

    # Crear directorios temporales para este job
    job_id = str(uuid.uuid4())
    tmp_dir = tempfile.mkdtemp(prefix=f"job_{job_id[:8]}_")
    output_dir = os.path.join(tmp_dir, "output")
    upload_dir = os.path.join(tmp_dir, "uploads")
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(upload_dir, exist_ok=True)

    # Datos del formulario
    data = {
        "apollo_api": request.form.get("apollo_api", "").strip(),
        "lusha_api": request.form.get("lusha_api", "").strip(),
        "paises": request.form.getlist("paises"),
    }

    # Validar API key requerida
    api_field = connector["required_api"]
    if not data.get(api_field):
        label = "Apollo" if "apollo" in api_field else "Lusha"
        return jsonify({"error": f"API Key de {label} es requerida para este proceso"}), 400

    # Guardar archivos subidos y mapear rutas
    for file_field in connector["required_files"]:
        uploaded = request.files.get(file_field)
        if not uploaded or not uploaded.filename:
            return jsonify({"error": f"Archivo requerido: {file_field.replace('_file', '').replace('_', ' ')}"}), 400
        save_path = os.path.join(upload_dir, f"{file_field}.csv")
        uploaded.save(save_path)
        key = file_field.replace("_file", "_path")  # e.g. "empresas_file" → "empresas_path"
        data[key] = save_path

    # Validar países si es necesario
    if connector["needs_countries"] and not data["paises"]:
        return jsonify({"error": "Selecciona al menos un país"}), 400

    # Registrar job
    log_q: queue.Queue = queue.Queue()
    stop_event = threading.Event()
    jobs[job_id] = {
        "queue": log_q,
        "stop_event": stop_event,
        "output_dir": output_dir,
        "output_filename": connector["output_filename"],
        "done": False,
        "process_type": process_type,
    }

    # Lanzar hilo
    t = threading.Thread(target=_run_job, args=(job_id, process_type, data), daemon=True)
    t.start()

    return jsonify({"job_id": job_id})


@app.route("/api/stream/<job_id>")
def stream(job_id: str):
    """SSE endpoint — envía logs en tiempo real al navegador."""
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job no encontrado"}), 404

    def generate():
        log_q: queue.Queue = job["queue"]
        while True:
            try:
                msg = log_q.get(timeout=25)
                if msg is None:
                    yield "data: __DONE__\n\n"
                    break
                # Escapar saltos de línea para no romper el protocolo SSE
                safe = str(msg).replace("\r", "").replace("\n", "\\n")
                yield f"data: {safe}\n\n"
            except queue.Empty:
                yield "data: __PING__\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/download/<job_id>")
def download(job_id: str):
    """Descarga el CSV de resultados del job."""
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job no encontrado"}), 404

    output_path = os.path.join(job["output_dir"], job["output_filename"])
    if not os.path.exists(output_path):
        return jsonify({"error": "Archivo no generado. El proceso puede no haber encontrado resultados."}), 404

    return send_file(output_path, as_attachment=True, download_name=job["output_filename"])


@app.route("/api/cancel/<job_id>", methods=["POST"])
def cancel(job_id: str):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job no encontrado"}), 404
    job["stop_event"].set()
    return jsonify({"status": "cancelado"})


@app.route("/api/status/<job_id>")
def status(job_id: str):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job no encontrado"}), 404
    output_path = os.path.join(job["output_dir"], job["output_filename"])
    return jsonify({
        "done": job["done"],
        "has_output": os.path.exists(output_path),
    })


# ================================================================
# ENTRY POINT
# ================================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)

import requests
import json
import csv
import os
import time
import uuid # Para generar el requestId
import urllib3 # <--- AÑADIDO

# --- DESHABILITAR ADVERTENCIAS DE SSL ---
# Esto es necesario al usar verify=False en redes corporativas
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURACIÓN ---
API_URL = "https://api.lusha.com/prospecting/company/enrich"
# Lusha permite enviar múltiples IDs a la vez. Un lote de 20 es un número seguro y eficiente.
BATCH_SIZE = 20

# Columnas que se incluirán en el archivo CSV
CSV_HEADERS = [
    'id', 'name', 'employees_min', 'employees_max', 'employees_range', 
    'revenue_min', 'revenue_max', 'website', 'founded_year', 
    'domain_homepage', 'domain_email', 'sic_description', 
    'naics_description', 'city', 'country', 'country_iso', 'continent', 
    'raw_location', 'linkedin_url', 'specialities', 'main_industry', 
    'sub_industry', 'technologies'
]

# --- FUNCIONES AUXILIARES ---

def safe_get(dct, *keys):
    """
    Navega de forma segura por un diccionario o lista anidado.
    Devuelve None si alguna clave no existe.
    """
    for key in keys:
        if isinstance(dct, list):
            try:
                dct = dct[key]
            except (IndexError, TypeError):
                return None
        elif isinstance(dct, dict):
            dct = dct.get(key)
        else:
            return None
            
        if dct is None:
            return None
    return dct

def extract_company_data(comp):
    """
    "Aplana" el objeto JSON de una compañía para guardarlo en el CSV.
    """
    # Extraer y unir listas complejas
    technologies_list = safe_get(comp, 'technologies')
    techs = '; '.join([t.get('name') for t in technologies_list if t.get('name')]) if technologies_list else None
    
    specialities_list = safe_get(comp, 'specialities')
    specs = '; '.join(specialities_list) if specialities_list else None

    return {
        'id': safe_get(comp, 'id'),
        'name': safe_get(comp, 'name'),
        'employees_min': safe_get(comp, 'companySize', 'min'),
        'employees_max': safe_get(comp, 'companySize', 'max'),
        'employees_range': safe_get(comp, 'employees'),
        'revenue_min': safe_get(comp, 'revenueRange', 0),
        'revenue_max': safe_get(comp, 'revenueRange', 1),
        'website': safe_get(comp, 'fqdn'),
        'founded_year': safe_get(comp, 'founded'),
        'domain_homepage': safe_get(comp, 'domains', 'homepage'),
        'domain_email': safe_get(comp, 'domains', 'email'),
        'sic_description': safe_get(comp, 'industryPrimaryGroupDetails', 'sics', 0, 'description'),
        'naics_description': safe_get(comp, 'industryPrimaryGroupDetails', 'naics', 0, 'description'),
        'city': safe_get(comp, 'city'),
        'country': safe_get(comp, 'country'),
        'country_iso': safe_get(comp, 'countryIso2'),
        'continent': safe_get(comp, 'continent'),
        'raw_location': safe_get(comp, 'rawLocation'),
        'linkedin_url': safe_get(comp, 'social', 'linkedin'),
        'specialities': specs,
        'main_industry': safe_get(comp, 'mainIndustry'),
        'sub_industry': safe_get(comp, 'subIndustry'),
        'technologies': techs
    }

def load_organization_ids_from_csv(filepath, log_callback):
    """
    Carga los IDs de organizaciones desde la primera columna de un archivo CSV.
    Asume que la primera fila es un encabezado y la salta.
    """
    organization_ids = []
    try:
        with open(filepath, mode='r', encoding='utf-8-sig') as file: # 'utf-8-sig' maneja BOM
            reader = csv.reader(file)
            next(reader) # Saltar el encabezado
            for row in reader:
                if row: # Asegurarse de que la fila no esté vacía
                    id_limpio = row[0].strip()
                    if id_limpio:
                        organization_ids.append(id_limpio)
        
        if not organization_ids:
            log_callback("⚠  Advertencia: El archivo CSV de IDs está vacío o no se encontraron IDs.")
        return organization_ids
    except FileNotFoundError:
        log_callback(f"❌ ERROR FATAL: No se encontró el archivo CSV de IDs en: {filepath}")
        return None
    except Exception as e:
        log_callback(f"❌ ERROR FATAL: No se pudo leer el archivo CSV de IDs. Causa: {e}")
        return None

# --- PROCESO PRINCIPAL ---

def run(api_key, organization_ids_csv_path, output_folder, log_callback, stop_event):
    """
    Recorre la lista de IDs de un CSV, consulta la API de Lusha y guarda los resultados.
    """
    
    # 1. Cargar los IDs desde el CSV
    log_callback("Cargando IDs de organizaciones desde el archivo CSV...")
    organization_ids = load_organization_ids_from_csv(organization_ids_csv_path, log_callback)
    
    if not organization_ids:
        log_callback("🛑 Proceso detenido. No se pudieron cargar los IDs de organizaciones.")
        return

    # 2. Definir archivo de salida
    output_csv_file = os.path.join(output_folder, 'lusha_organizations_output.csv')
    
    log_callback(f"🚀 Iniciando la extracción de {len(organization_ids)} organizaciones desde Lusha...")
    log_callback(f"Los resultados se guardarán en: {output_csv_file}")

    all_results = []
    
    try:
        # 3. Dividir los IDs en lotes
        for i in range(0, len(organization_ids), BATCH_SIZE):
            
            # Verificar señal de detención antes de cada lote
            if stop_event.is_set():
                log_callback("🛑 Proceso cancelado por el usuario.")
                break
            
            batch_ids = organization_ids[i:i + BATCH_SIZE]
            
            log_callback(f"\nConsultando lote {i//BATCH_SIZE + 1} de {len(organization_ids)//BATCH_SIZE + 1} (IDs: {', '.join(batch_ids[:3])}...)")

            # 4. Preparar payload y headers
            payload = json.dumps({
                "requestId": str(uuid.uuid4()), # Genera un ID único para la solicitud
                "companiesIds": batch_ids
            })
            
            headers = {
                'Content-Type': 'application/json',
                'api_key': api_key # Usa la API key del argumento
                # No incluir la cookie de Postman, no es necesaria y fallaría
            }

            # 5. Realizar la llamada a la API
            try:
                # --- CAMBIO AQUÍ: Añadido verify=False ---
                response = requests.request("POST", API_URL, headers=headers, data=payload, verify=False)
                
                # --- CAMBIO AQUÍ: Aceptar 200 (OK) y 201 (Created) como éxito ---
                if response.status_code == 200 or response.status_code == 201:
                    data = response.json()
                    companies_list = data.get('companies', [])
                    log_callback(f"✔ Lote procesado. Se encontraron {len(companies_list)} organizaciones.")
                    
                    for company_data in companies_list:
                        flattened_data = extract_company_data(company_data)
                        all_results.append(flattened_data)
                        
                elif response.status_code == 401 or response.status_code == 403:
                    log_callback(f"❌ ERROR DE AUTENTICACIÓN (Lote {i//BATCH_SIZE + 1}): {response.status_code}. Revisa tu API Key.")
                    log_callback(f"Respuesta: {response.text}")
                    break # Detener el proceso si la API key es inválida
                else:
                    log_callback(f"❌ Error HTTP en lote {i//BATCH_SIZE + 1}: {response.status_code} {response.reason}")
                    log_callback(f"Respuesta: {response.text}")

            except requests.exceptions.RequestException as e:
                log_callback(f"❌ Ocurrió un error de conexión en el lote {i//BATCH_SIZE + 1}: {e}")
            
            # Pausa de 1 segundo entre solicitudes para no sobrecargar la API
            time.sleep(1)

        # 6. Escribir todos los resultados al CSV
        if all_results:
            log_callback("\nEscribiendo resultados en el archivo CSV...")
            with open(output_csv_file, mode='w', newline='', encoding='utf-8') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=CSV_HEADERS)
                writer.writeheader()
                writer.writerows(all_results)
        else:
            log_callback("\nNo se encontraron resultados para escribir en el CSV.")

    except IOError as e:
        log_callback(f"❌ ERROR FATAL: No se pudo escribir en el archivo de salida. Causa: {e}")
        return
    except Exception as e:
        log_callback(f"❌ ERROR INESPERADO: {e}")
        return

    if stop_event.is_set():
        log_callback(f"\n🚫 Proceso cancelado. Se guardaron {len(all_results)} resultados parciales en '{output_csv_file}'.")
    else:
        log_callback(f"\n🎉 ¡Proceso completado! Se encontraron {len(all_results)} organizaciones. Revisa '{output_csv_file}'.")

# --- Fin del script (No se necesita __main__) ---
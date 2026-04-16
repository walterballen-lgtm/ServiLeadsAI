import requests
import json
import csv
import os
import time

# --- FUNCIÓN DE EXTRACCIÓN DE DATOS (Sin cambios) ---
# Columnas que se incluirán en el archivo CSV, en el orden especificado
CSV_HEADERS = [
    'ID_BUSCADO', 'id', 'name', 'website_url', 'linkedin_url', 'twitter_url', 
    'facebook_url', 'number', 'sanitized_number', 'founded_year', 
    'primary_domain', 'industry', 'estimated_num_employees', 
    'organization_revenue_printed', 'organization_revenue', 'raw_address', 
    'city', 'postal_code', 'country', 'annual_revenue_printed', 'annual_revenue'
]

def extract_organization_data(json_data, searched_id):
    """
    Extrae y aplana los datos de la organización desde la respuesta JSON de la API.
    """
    org = json_data.get('organization', {})
    if not org:
        return None # Si no hay datos de organización, devuelve None

    # El método .get(key, default_value) se usa para evitar errores si una clave no existe
    primary_phone = org.get('primary_phone', {}) or {} # Asegura que primary_phone sea un dict

    extracted_data = {
        'ID_BUSCADO': searched_id,
        'id': org.get('id'),
        'name': org.get('name'),
        'website_url': org.get('website_url'),
        'linkedin_url': org.get('linkedin_url'),
        'twitter_url': org.get('twitter_url'),
        'facebook_url': org.get('facebook_url'),
        'number': primary_phone.get('number'),
        'sanitized_number': primary_phone.get('sanitized_number'),
        'founded_year': org.get('founded_year'),
        'primary_domain': org.get('primary_domain'),
        'industry': org.get('industry'),
        'estimated_num_employees': org.get('estimated_num_employees'),
        'organization_revenue_printed': org.get('organization_revenue_printed'),
        'organization_revenue': org.get('organization_revenue'),
        'raw_address': org.get('raw_address'),
        'city': org.get('city'),
        'postal_code': org.get('postal_code'),
        'country': org.get('country'),
        'annual_revenue_printed': org.get('annual_revenue_printed'),
        'annual_revenue': org.get('annual_revenue'),
    }
    return extracted_data

# --- FUNCIÓN AUXILIAR PARA CARGAR IDs ---

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
                    organization_ids.append(row[0].strip())
        
        if not organization_ids:
            log_callback("⚠  Advertencia: El archivo CSV de IDs está vacío o no se encontraron IDs.")
        return organization_ids
    except FileNotFoundError:
        log_callback(f"❌ ERROR FATAL: No se encontró el archivo CSV de IDs en: {filepath}")
        return None
    except Exception as e:
        log_callback(f"❌ ERROR FATAL: No se pudo leer el archivo CSV de IDs. Causa: {e}")
        return None

# --- PROCESO PRINCIPAL (AHORA `run`) ---

def run(api_key, organization_ids_csv_path, output_folder, log_callback, stop_event):
    """
    Recorre la lista de IDs de un CSV, consulta la API y guarda los resultados.
    """
    
    # 1. Cargar los IDs desde el CSV
    log_callback("Cargando IDs de organizaciones desde el archivo CSV...")
    organization_ids = load_organization_ids_from_csv(organization_ids_csv_path, log_callback)
    
    if not organization_ids:
        log_callback("🛑 Proceso detenido. No se pudieron cargar los IDs de organizaciones.")
        return

    # 2. Definir archivo de salida
    output_csv_file = os.path.join(output_folder, 'apollo_organizations_output.csv')
    
    log_callback(f"🚀 Iniciando la extracción de {len(organization_ids)} organizaciones...")
    log_callback(f"Los resultados se guardarán en: {output_csv_file}")

    # 3. Abrir el archivo CSV para escribir los datos
    try:
        with open(output_csv_file, mode='w', newline='', encoding='utf-8') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=CSV_HEADERS)
            writer.writeheader() # Escribir la fila de encabezado

            for org_id in organization_ids:
                
                # --- CAMBIO: Verificar señal de detención ---
                if stop_event.is_set():
                    log_callback("🛑 Proceso cancelado por el usuario.")
                    break
                
                # Construir la URL para el ID actual
                url = f"https://api.apollo.io/api/v1/organizations/{org_id}"
                
                # --- CAMBIO: Usar la api_key del argumento ---
                params = {'api_key': api_key}
                headers = {
                    'Cache-Control': 'no-cache',
                    'Content-Type': 'application/json',
                    'x-api-key': api_key, # --- CAMBIO
                    'accept': 'application/json'
                }
                
                log_callback(f"\nConsultando ID: {org_id}...")
                
                try:
                    response = requests.get(url, headers=headers, params=params)
                    # Lanzará un error si la respuesta es 4xx o 5xx
                    response.raise_for_status() 
                    
                    data = response.json()
                    
                    # Extraer y aplanar los datos del JSON
                    extracted_info = extract_organization_data(data, org_id)
                    
                    if extracted_info:
                        # Escribir la fila de datos en el CSV
                        writer.writerow(extracted_info)
                        log_callback(f"✔ Datos de '{extracted_info.get('name', 'N/A')}' guardados.")
                    else:
                        log_callback(f"⚠  Advertencia: No se encontraron datos de organización en la respuesta para el ID {org_id}.")

                except requests.exceptions.HTTPError as e:
                    log_callback(f"❌ Error HTTP para el ID {org_id}: {e.response.status_code} {e.response.reason}")
                    # Escribir una fila de error para saber cuál falló
                    writer.writerow({'ID_BUSCADO': org_id, 'name': f'ERROR: {e.response.status_code}'})
                except requests.exceptions.RequestException as e:
                    log_callback(f"❌ Ocurrió un error de conexión para el ID {org_id}: {e}")
                    writer.writerow({'ID_BUSCADO': org_id, 'name': 'ERROR DE CONEXIÓN'})
                
                # Pausa de 1 segundo entre solicitudes para no sobrecargar la API
                time.sleep(1)

    except IOError as e:
        log_callback(f"❌ ERROR FATAL: No se pudo escribir en el archivo de salida. Causa: {e}")
        return
    except Exception as e:
        log_callback(f"❌ ERROR INESPERADO: {e}")
        return

    if stop_event.is_set():
        log_callback(f"\n🚫 Proceso cancelado. Los resultados parciales se guardaron en '{output_csv_file}'.")
    else:
        log_callback(f"\n🎉 ¡Proceso completado! Los datos han sido guardados en el archivo '{output_csv_file}'.")

# --- Fin del script (No se necesita __main__) ---

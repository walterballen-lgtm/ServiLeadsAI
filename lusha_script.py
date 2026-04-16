import requests
import json
import csv
import os
import time

# Rate limiting
class RateLimiter:
    """Limitador de velocidad para respetar límites de API"""
    def __init__(self, requests_per_minute=60):
        self.requests_per_minute = requests_per_minute
        self.min_interval = 60 / requests_per_minute
        self.last_request = None
    
    def wait(self):
        """Espera el tiempo necesario para respetar rate limit"""
        if self.last_request:
            from datetime import datetime
            elapsed = (datetime.now() - self.last_request).total_seconds()
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
        from datetime import datetime
        self.last_request = datetime.now()

def run(api_key, empresas, cargos, paises, output_folder, log_callback, stop_event):
    """
    Ejecuta el proceso de extracción de Lusha.
    
    Args:
        api_key (str): La clave de API para Lusha.
        empresas (list): Lista de nombres de empresas.
        cargos (list): Lista de cargos a buscar.
        paises (list): Lista de países seleccionados.
        output_folder (str): Ruta de la carpeta para guardar el resultado.
        log_callback (function): Función para enviar mensajes a la consola de la GUI.
        stop_event (threading.Event): Evento para señalar la cancelación.
    """
    log_callback("🚀 Iniciando búsqueda filtrada de contactos en Lusha...")
    
    url = "https://api.lusha.com/prospecting/contact/search"
    output_file = os.path.join(output_folder, "resultados_lusha.csv")
    
    # Rate limiter (30 requests por minuto para Lusha)
    limiter = RateLimiter(requests_per_minute=30)
    
    fieldnames = [
        'empresa_buscada', 'pais_buscado', 'name', 'contactId', 'jobTitle', 'companyId', 'companyName', 'fqdn',
        'personId', 'logoUrl', 'hasEmails', 'hasPhones', 'hasDirectPhone', 'hasWorkEmail', 'hasPrivateEmail',
        'hasMobilePhone', 'hasSocialLink'
    ]

    try:
        with open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            log_callback(f"✅ Archivo de salida '{os.path.basename(output_file)}' creado.")

            for empresa in empresas:
                # --- CAMBIO: Chequear cancelación antes de cada empresa ---
                if stop_event.is_set():
                    log_callback("🛑 Proceso cancelado por el usuario.")
                    break # Sale del bucle de empresas
                
                for pais in paises:
                    # --- CAMBIO: Chequear cancelación antes de cada país ---
                    if stop_event.is_set():
                        log_callback(f"🛑 Cancelado. Omitiendo países restantes para {empresa}.")
                        break # Sale del bucle de países

                    log_callback(f"\n🔎 Buscando en Lusha: {empresa} en {pais}...")
                    
                    page = 0  # PAGINACIÓN: Iniciar en página 0
                    total_pagina = 0
                    
                    while True:  # PAGINACIÓN: Loop para obtener todas las páginas
                        if stop_event.is_set():
                            break
                        
                        # Aplicar rate limiting
                        limiter.wait()
                        
                        payload = {
                            "pages": {"page": page, "size": 50},  # PAGINACIÓN: Página dinámica
                            "filters": {
                                "contacts": {
                                    "include": {
                                        "jobTitles": cargos,
                                        "locations": [{"country": pais}],
                                        "existing_data_points": ["phone", "work_email", "mobile_phone"]
                                    }
                                },
                                "companies": {"include": {"names": [empresa]}}
                            }
                        }
                        headers = {'Content-Type': 'application/json', 'api_key': api_key}

                        try:
                            response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30, verify=True)
                            data = response.json()

                            if isinstance(data, dict):
                                if response.status_code not in [200, 201]:
                                    log_callback(f"  -> ⚠️ Advertencia: Error {response.status_code}: {response.text}")
                                    break
                                
                                contacts = data.get('data', [])
                                if not contacts:
                                    if page == 0:
                                        log_callback("  -> No se encontraron contactos que cumplan los filtros.")
                                    break  # PAGINACIÓN: Salir si no hay más resultados
                                
                                log_callback(f"  -> Página {page + 1}: {len(contacts)} contactos encontrados.")
                                total_pagina += len(contacts)
                                
                                for contact in contacts:
                                    writer.writerow({
                                        'empresa_buscada': empresa, 'pais_buscado': pais,
                                        'name': contact.get('name', 'N/A'),
                                        'contactId': contact.get('contactId', 'N/A'),
                                        'jobTitle': contact.get('jobTitle', 'N/A'),
                                        'companyId': contact.get('companyId', 'N/A'),
                                        'companyName': contact.get('companyName', 'N/A'),
                                        'fqdn': contact.get('fqdn', 'N/A'),
                                        'personId': contact.get('personId', 'N/A'),
                                        'logoUrl': contact.get('logoUrl', 'N/A'),
                                        'hasEmails': contact.get('hasEmails', False),
                                        'hasPhones': contact.get('hasPhones', False),
                                        'hasDirectPhone': contact.get('hasDirectPhone', False),
                                        'hasWorkEmail': contact.get('hasWorkEmail', False),
                                        'hasPrivateEmail': contact.get('hasPrivateEmail', False),
                                        'hasMobilePhone': contact.get('hasMobilePhone', False),
                                        'hasSocialLink': contact.get('hasSocialLink', False)
                                    })
                                
                                page += 1  # PAGINACIÓN: Siguiente página
                            else:
                                log_callback("  -> No se encontraron contactos (respuesta en formato de lista).")
                                break
                        except requests.exceptions.RequestException as e:
                            log_callback(f"  -> ❌ Error de conexión: {e}")
                            break
                        except json.JSONDecodeError:
                            log_callback("  -> ❌ Error: La respuesta no es un JSON válido.")
                            break
                    
                    if total_pagina > 0:
                        log_callback(f"  -> Total para {empresa} en {pais}: {total_pagina} contactos")
                    
                    # Salir si se cancela durante la petición
                    if stop_event.is_set():
                        break
                    time.sleep(1)

        # --- CAMBIO: Mensaje final condicional ---
        if stop_event.is_set():
            log_callback(f"\n🚫 Proceso de Lusha cancelado. El archivo '{os.path.basename(output_file)}' puede estar incompleto.")
        else:
            log_callback(f"\n✅ Proceso de Lusha completado. Revisa el archivo '{os.path.basename(output_file)}'.")

    except IOError as e:
        log_callback(f"❌ ERROR FATAL: No se pudo escribir en el archivo de salida. Causa: {e}")
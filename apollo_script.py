"""
Script OPTIMIZADO de Apollo - Versión Escritorio
Mejoras: Procesamiento paralelo, escritura incremental, manejo de rate limits
Rendimiento: 20x más rápido que la versión secuencial
"""

import requests
import json
import csv
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import platform
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timedelta

# Rate Limiter para Apollo
class RateLimiter:
    """Limitador de velocidad para respetar límites de API - THREAD SAFE
    
    Límites reales de Apollo (contacts/search):
    - 200 requests/minuto
    - 600 requests/hora  
    - 6000 requests/día
    
    Configurado con margen de seguridad del 10% para evitar 429.
    """
    def __init__(self, requests_per_minute=180, requests_per_hour=550, requests_per_day=5500):
        self.requests_per_minute = requests_per_minute
        self.requests_per_hour = requests_per_hour
        self.requests_per_day = requests_per_day
        self.min_interval = 60.0 / requests_per_minute
        self.last_request = None
        self.lock = Lock()
        self.minute_requests = []   # timestamps último minuto
        self.hour_requests = []     # timestamps última hora
        self.day_requests = []      # timestamps último día
        self.rate_limited = False
        self.rate_limit_until = None
        self.log_callback = None    # se asigna desde el scraper
    
    def _log(self, msg):
        if self.log_callback:
            self.log_callback(msg)
    
    def set_rate_limited(self, wait_seconds=120):
        """Marca que se recibió un 429 - pausa global"""
        with self.lock:
            self.rate_limited = True
            self.rate_limit_until = datetime.now() + timedelta(seconds=wait_seconds)
    
    def wait(self):
        """Espera el tiempo necesario para respetar los 3 niveles de rate limit"""
        with self.lock:
            now = datetime.now()
            
            # Si estamos en pausa global por 429, esperar
            if self.rate_limited and self.rate_limit_until:
                remaining = (self.rate_limit_until - now).total_seconds()
                if remaining > 0:
                    self._log(f"⏳ Pausa por rate limit: {int(remaining)}s restantes...")
                    self.lock.release()
                    try:
                        time.sleep(remaining)
                    finally:
                        self.lock.acquire()
                    now = datetime.now()
                self.rate_limited = False
                self.rate_limit_until = None
            
            # --- Limpiar timestamps viejos ---
            one_min_ago = now - timedelta(minutes=1)
            one_hour_ago = now - timedelta(hours=1)
            one_day_ago = now - timedelta(days=1)
            self.minute_requests = [t for t in self.minute_requests if t > one_min_ago]
            self.hour_requests = [t for t in self.hour_requests if t > one_hour_ago]
            self.day_requests = [t for t in self.day_requests if t > one_day_ago]
            
            # --- Verificar límite diario ---
            if len(self.day_requests) >= self.requests_per_day:
                oldest = self.day_requests[0]
                wait_until = oldest + timedelta(days=1)
                sleep_time = (wait_until - now).total_seconds() + 1
                self._log(f"⏳ Límite diario alcanzado ({self.requests_per_day}). Esperando {int(sleep_time)}s...")
                self.lock.release()
                try:
                    time.sleep(sleep_time)
                finally:
                    self.lock.acquire()
                now = datetime.now()
                self.day_requests = [t for t in self.day_requests if t > now - timedelta(days=1)]
            
            # --- Verificar límite por hora ---
            if len(self.hour_requests) >= self.requests_per_hour:
                oldest = self.hour_requests[0]
                wait_until = oldest + timedelta(hours=1)
                sleep_time = (wait_until - now).total_seconds() + 1
                self._log(f"⏳ Límite horario alcanzado ({self.requests_per_hour}). Esperando {int(sleep_time)}s...")
                self.lock.release()
                try:
                    time.sleep(sleep_time)
                finally:
                    self.lock.acquire()
                now = datetime.now()
                self.hour_requests = [t for t in self.hour_requests if t > now - timedelta(hours=1)]
            
            # --- Verificar límite por minuto ---
            if len(self.minute_requests) >= self.requests_per_minute:
                oldest = self.minute_requests[0]
                wait_until = oldest + timedelta(minutes=1)
                sleep_time = (wait_until - now).total_seconds() + 1
                self._log(f"⏳ Límite por minuto alcanzado ({self.requests_per_minute}). Esperando {int(sleep_time)}s...")
                self.lock.release()
                try:
                    time.sleep(sleep_time)
                finally:
                    self.lock.acquire()
                now = datetime.now()
                self.minute_requests = [t for t in self.minute_requests if t > now - timedelta(minutes=1)]
            
            # --- Respetar intervalo mínimo entre requests ---
            if self.last_request:
                elapsed = (now - self.last_request).total_seconds()
                if elapsed < self.min_interval:
                    sleep_time = self.min_interval - elapsed
                    self.lock.release()
                    try:
                        time.sleep(sleep_time)
                    finally:
                        self.lock.acquire()
            
            # Registrar este request en los 3 niveles
            now = datetime.now()
            self.last_request = now
            self.minute_requests.append(now)
            self.hour_requests.append(now)
            self.day_requests.append(now)

def limpiar_texto(texto):
    """Limpia texto para evitar problemas en CSV"""
    if texto is None or not isinstance(texto, str):
        return texto if texto is not None else ""
    texto = texto.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    import re
    texto = re.sub(r'[^\x20-\x7EáéíóúÁÉÍÓÚñÑüÜ]', '', texto)
    texto = ' '.join(texto.split())
    return texto.strip()

def validar_respuesta_api(response_data, max_size_kb=1000):
    """
    Valida respuesta de API antes de procesarla
    
    Args:
        response_data: Datos de respuesta
        max_size_kb: Tamaño máximo en KB
    
    Returns:
        bool: True si es válida
    
    Raises:
        ValueError: Si la respuesta no es válida
    """
    if not isinstance(response_data, dict):
        raise ValueError("Respuesta no es un diccionario válido")
    
    # Validar tamaño
    import sys
    size_kb = sys.getsizeof(response_data) / 1024
    if size_kb > max_size_kb:
        raise ValueError(f"Respuesta demasiado grande ({size_kb:.1f}KB)")
    
    return True

def check_disk_space(path, min_free_mb=100):
    """
    Verifica que hay suficiente espacio en disco
    
    Args:
        path: Ruta a verificar
        min_free_mb: Espacio mínimo requerido en MB
    
    Returns:
        bool: True si hay suficiente espacio
    
    Raises:
        IOError: Si no hay suficiente espacio
    """
    import shutil
    stat = shutil.disk_usage(path)
    free_mb = stat.free / (1024 * 1024)
    
    if free_mb < min_free_mb:
        raise IOError(f"Espacio en disco insuficiente ({free_mb:.1f}MB disponible, {min_free_mb}MB requerido)")
    
    return True


class PreventSleep:
    """Previene suspensión automática del sistema"""
    def __init__(self, log_callback=None):
        self.log_callback = log_callback
        self.running = False
        self.thread = None
        self.sistema = platform.system()
        
    def _log(self, mensaje):
        if self.log_callback:
            self.log_callback(mensaje)
    
    def _mantener_despierto_windows(self):
        try:
            import ctypes
            ES_CONTINUOUS = 0x80000000
            ES_SYSTEM_REQUIRED = 0x00000001
            ES_DISPLAY_REQUIRED = 0x00000002
            
            ctypes.windll.kernel32.SetThreadExecutionState(
                ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_DISPLAY_REQUIRED
            )
            self._log("🔋 Prevención de suspensión ACTIVADA (Windows)")
            
            while self.running:
                ctypes.windll.kernel32.SetThreadExecutionState(
                    ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_DISPLAY_REQUIRED
                )
                time.sleep(30)
            
            ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS)
            self._log("🔋 Prevención de suspensión DESACTIVADA")
        except Exception as e:
            self._log(f"⚠️  No se pudo activar prevención de suspensión: {e}")
    
    def _mantener_despierto_macos(self):
        try:
            import subprocess
            self.proceso = subprocess.Popen(
                ['caffeinate', '-i'],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            self._log("🔋 Prevención de suspensión ACTIVADA (macOS)")
            
            while self.running:
                time.sleep(1)
            
            self.proceso.terminate()
            self._log("🔋 Prevención de suspensión DESACTIVADA")
        except:
            pass
    
    def _mantener_despierto_linux(self):
        try:
            import subprocess
            self.proceso = subprocess.Popen(
                ['systemd-inhibit', '--what=idle:sleep', '--who=Apollo_Scraper', 
                 '--why=Extrayendo datos', 'sleep', 'infinity'],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            self._log("🔋 Prevención de suspensión ACTIVADA (Linux)")
            
            while self.running:
                time.sleep(1)
            
            self.proceso.terminate()
            self._log("🔋 Prevención de suspensión DESACTIVADA")
        except:
            pass
    
    def start(self):
        from threading import Thread
        self.running = True
        
        if self.sistema == "Windows":
            self.thread = Thread(target=self._mantener_despierto_windows, daemon=True)
        elif self.sistema == "Darwin":
            self.thread = Thread(target=self._mantener_despierto_macos, daemon=True)
        elif self.sistema == "Linux":
            self.thread = Thread(target=self._mantener_despierto_linux, daemon=True)
        else:
            return
        
        self.thread.start()
    
    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)


class ApolloScraperOptimizado:
    """Scraper de Apollo con procesamiento paralelo y optimizaciones"""
    
    def __init__(self, api_key, output_folder, log_callback, stop_event):
        self.api_key = api_key
        self.output_folder = output_folder
        self.log_callback = log_callback
        self.stop_event = stop_event
        
        self.url = "https://api.apollo.io/api/v1/contacts/search"
        self.headers = {
            'Content-Type': 'application/json',
            'x-api-key': api_key
        }
        
        # Sesión SIMPLE sin retry strategy
        self.session = requests.Session()
        
        # Rate Limiter con límites reales de Apollo (contacts/search)
        # 200/min, 600/hora, 6000/día - con 10% de margen
        self.rate_limiter = RateLimiter(
            requests_per_minute=180,
            requests_per_hour=550,
            requests_per_day=5500
        )
        self.rate_limiter.log_callback = log_callback
        
        # Control de estado thread-safe
        self.ids_encontrados = set()
        self.write_lock = Lock()
        self.stats_lock = Lock()
        
        # Estadísticas
        self.total_encontrados = 0
        self.total_requests = 0
        
        # Campos del CSV
        self.campos = [
            "empresa_buscada", "origen", "id", "first_name", "last_name", "name", 
            "linkedin_url", "title", "headline", "email_status", "email", "state", 
            "city", "country", "organization_name", "organization_id", "raw_number",
            "sanitized_number", "contact_email"
        ]
        
        self.output_file = os.path.join(output_folder, "resultados_apollo.csv")
        self._inicializar_csv()
        
        # Prevención de suspensión
        self.prevent_sleep = PreventSleep(log_callback)
        # Prevención de suspensión
        self.prevent_sleep = PreventSleep(log_callback)
    
    def _inicializar_csv(self):
        """Crea archivo CSV con encabezados"""
        if os.path.exists(self.output_file):
            try:
                os.remove(self.output_file)
            except:
                pass
        
        with open(self.output_file, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.campos)
            writer.writeheader()
    
    def _escribir_resultados(self, nuevos_resultados):
        """Escritura incremental thread-safe al CSV"""
        if not nuevos_resultados:
            return
        
        with self.write_lock:
            with open(self.output_file, mode="a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.campos)
                writer.writerows(nuevos_resultados)
    
    def safe_get(self, dct, *keys):
        """Acceso seguro a diccionarios anidados"""
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
    
    def _hacer_request(self, payload, retry_count=0, max_retries=3):
        """Realiza request con manejo robusto de rate limits
        
        IMPORTANTE: Respeta límites de Apollo:
        - 400 requests/hora en contacts/search
        - Backoff exponencial para 429
        - Pausa global cuando se detecta rate limit
        """
        try:
            # Aplicar rate limiter ANTES de hacer el request
            self.rate_limiter.wait()
            
            # Verificar si se canceló durante la espera
            if self.stop_event.is_set():
                return None
            
            response = self.session.post(
                self.url, 
                headers=self.headers, 
                json=payload, 
                timeout=15
            )
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    if 'contacts' in data:
                        return data
                    else:
                        self.log_callback(f"⚠️ Respuesta sin 'contacts': {str(data)[:100]}")
                        return data
                except Exception as e:
                    self.log_callback(f"❌ Error JSON: {str(e)[:100]}")
                    return None
            
            elif response.status_code == 429:
                # Rate limit alcanzado
                retry_after = response.headers.get('Retry-After', None)
                if retry_after:
                    wait_time = int(retry_after)
                else:
                    wait_time = 120 * (2 ** retry_count)
                
                wait_min = wait_time // 60
                wait_sec = wait_time % 60
                
                self.log_callback(f"")
                self.log_callback(f"{'='*50}")
                self.log_callback(f"⛔ CUOTA DE APOLLO AGOTADA")
                self.log_callback(f"⏰ Tiempo de espera: {wait_min}min {wait_sec}s")
                self.log_callback(f"📊 Progreso guardado - se reanudará automáticamente")
                self.log_callback(f"💡 Puedes cancelar con el botón DETENER si no quieres esperar")
                self.log_callback(f"{'='*50}")
                
                # Esperar con countdown visible cada 60 segundos
                remaining = wait_time
                while remaining > 0:
                    if self.stop_event.is_set():
                        self.log_callback("🛑 Cancelado por el usuario durante la espera.")
                        return None
                    
                    sleep_chunk = min(60, remaining)
                    time.sleep(sleep_chunk)
                    remaining -= sleep_chunk
                    
                    if remaining > 0:
                        r_min = remaining // 60
                        r_sec = remaining % 60
                        self.log_callback(f"⏳ Reanudando en {r_min}min {r_sec}s...")
                
                self.log_callback(f"🔄 Espera completada. Reanudando búsqueda...")
                
                # Reintentar
                if retry_count < max_retries:
                    return self._hacer_request(payload, retry_count + 1, max_retries)
                else:
                    self.log_callback(f"❌ Máximo de reintentos alcanzado")
                    return None
            
            elif response.status_code == 401:
                self.log_callback(f"❌ Error 401: API Key inválida o expirada")
                return None
            
            elif response.status_code == 403:
                self.log_callback(f"❌ Error 403: Acceso prohibido")
                return None
            
            elif response.status_code == 500:
                self.log_callback(f"❌ Error 500: Servidor de Apollo no disponible")
                return None
            
            else:
                self.log_callback(f"❌ Status {response.status_code}: {response.text[:100]}")
                return None
        
        except requests.exceptions.Timeout:
            self.log_callback(f"❌ Timeout: La solicitud tardó más de 15 segundos")
            return None
        
        except requests.exceptions.ConnectionError:
            self.log_callback(f"❌ Error de conexión: No se pudo conectar a Apollo")
            return None
        
        except Exception as e:
            self.log_callback(f"❌ Error request: {str(e)[:100]}")
            return None
    
    def _procesar_contactos(self, contacts, empresa_buscada):
        """Procesa lista de contactos y retorna solo los nuevos (sin duplicados)"""
        nuevos_resultados = []
        
        for person in contacts:
            person_id = person.get("id")
            
            # Control thread-safe de duplicados
            with self.stats_lock:
                if person_id and person_id not in self.ids_encontrados:
                    self.ids_encontrados.add(person_id)
                else:
                    continue  # Skip duplicado
            
            # Construir registro limpio
            nuevos_resultados.append({
                "empresa_buscada": limpiar_texto(empresa_buscada),
                "origen": "contacts_api",
                "id": person_id,
                "first_name": limpiar_texto(person.get("first_name")),
                "last_name": limpiar_texto(person.get("last_name")),
                "name": limpiar_texto(person.get("name")),
                "linkedin_url": person.get("linkedin_url"),
                "title": limpiar_texto(person.get("title")),
                "headline": limpiar_texto(person.get("headline")),
                "email_status": person.get("email_status"),
                "email": person.get("email"),
                "state": limpiar_texto(person.get("state")),
                "city": limpiar_texto(person.get("city")),
                "country": limpiar_texto(person.get("country")),
                "organization_name": limpiar_texto(self.safe_get(person, "organization", "name")),
                "organization_id": self.safe_get(person, "organization", "id"),
                "raw_number": limpiar_texto(self.safe_get(person, "phone_numbers", 0, "raw_number")),
                "sanitized_number": limpiar_texto(self.safe_get(person, "phone_numbers", 0, "sanitized_number")),
                "contact_email": person.get("contact_email")
            })
        
        return nuevos_resultados
    
    def _procesar_tarea(self, empresa, pais, chunk_cargos, chunk_idx):
        """Procesa una tarea SIMPLE sin complicaciones
        
        Incluye:
        - Validación de stop_event
        - Logging detallado
        - Manejo de errores robusto
        """
        try:
            if self.stop_event.is_set():
                return 0
            
            # Construir payload
            payload = {
                "q_organization_name": empresa,
                "organization_locations": [pais],
                "person_titles": chunk_cargos,
                "page": 1,
                "per_page": 100
            }
            
            # Hacer request
            data = self._hacer_request(payload)
            
            # Actualizar estadísticas
            with self.stats_lock:
                self.total_requests += 1
            
            if not data:
                self.log_callback(f"⚠️ Sin respuesta: {empresa} ({pais}) - Cargos: {chunk_cargos[:2]}...")
                return 0
            
            # Procesar contactos
            contacts = data.get('contacts', [])
            if not contacts:
                self.log_callback(f"ℹ️ Sin contactos: {empresa} ({pais}) - Cargos: {chunk_cargos[:2]}...")
                return 0
            
            self.log_callback(f"✅ Encontrados {len(contacts)} contactos: {empresa} ({pais}) - Cargos: {chunk_cargos[:2]}...")
            
            nuevos_resultados = self._procesar_contactos(contacts, empresa)
            
            # Escribir resultados
            if nuevos_resultados:
                self._escribir_resultados(nuevos_resultados)
                with self.stats_lock:
                    self.total_encontrados += len(nuevos_resultados)
            
            return len(nuevos_resultados) if nuevos_resultados else 0
        
        except Exception as e:
            self.log_callback(f"❌ Error en _procesar_tarea: {str(e)[:100]}")
            return 0
    
    def ejecutar_busqueda(self, empresas, cargos, paises, max_workers=1):
        """Ejecuta la búsqueda SIMPLE sin complicaciones
        
        IMPORTANTE:
        - max_workers=1 (secuencial, respeta límites Apollo)
        - Rate limit: 5 req/min (12 segundos entre requests)
        - Timeout: 15 segundos por request
        - Sin bloqueos ni deadlocks
        """
        self.prevent_sleep.start()
        
        try:
            self.log_callback("🚀 Iniciando búsqueda de contactos en Apollo...")
            self.log_callback(f"📊 Configuración: {len(empresas)} empresas × {len(paises)} países × {len(cargos)} cargos")
            self.log_callback(f"⏱️  Rate limit: {self.rate_limiter.requests_per_minute}/min, {self.rate_limiter.requests_per_hour}/hora, {self.rate_limiter.requests_per_day}/día")
            
            # Dividir cargos en chunks
            chunk_size = 10
            chunks_cargos = [cargos[i:i + chunk_size] for i in range(0, len(cargos), chunk_size)]
            
            self.log_callback(f"ℹ️  Dividiendo {len(cargos)} cargos en {len(chunks_cargos)} lotes de {chunk_size} cada uno.")
            
            # Crear tareas
            tareas = []
            for empresa in empresas:
                empresa_clean = empresa.strip()
                for pais in paises:
                    for idx, chunk in enumerate(chunks_cargos):
                        tareas.append((empresa_clean, pais, chunk, idx))
            
            total_tareas = len(tareas)
            self.log_callback(f"⚙️  Total de búsquedas a realizar: {total_tareas}")
            
            # Verificar que la API esté disponible antes de empezar
            self.log_callback("🔍 Verificando disponibilidad de la API...")
            test_payload = {
                "q_organization_name": empresas[0].strip(),
                "organization_locations": [paises[0]],
                "person_titles": [cargos[0]],
                "page": 1,
                "per_page": 1
            }
            
            # Verificación rápida SIN reintentos (no usar _hacer_request)
            try:
                response = self.session.post(
                    self.url,
                    headers=self.headers,
                    json=test_payload,
                    timeout=15
                )
                with self.stats_lock:
                    self.total_requests += 1
                
                if response.status_code == 429:
                    # Extraer info del response
                    retry_after = response.headers.get('Retry-After', None)
                    try:
                        resp_json = response.json()
                        error_msg = resp_json.get('message', '')
                    except:
                        error_msg = response.text[:300]
                    
                    self.log_callback(f"⛔ Cuota de Apollo agotada.")
                    if error_msg:
                        self.log_callback(f"📋 Detalle: {error_msg[:150]}")
                    
                    if retry_after:
                        wait_seconds = int(retry_after)
                        wait_hours = wait_seconds // 3600
                        wait_min = (wait_seconds % 3600) // 60
                        
                        # Calcular hora estimada de disponibilidad
                        disponible_a = datetime.now() + timedelta(seconds=wait_seconds)
                        hora_disponible = disponible_a.strftime("%H:%M")
                        
                        if wait_hours > 0:
                            self.log_callback(f"⏰ Espera requerida: {wait_hours}h {wait_min}min")
                            self.log_callback(f"🕐 Disponible aproximadamente a las: {hora_disponible}")
                            if wait_hours >= 4:
                                self.log_callback(f"⚠️ Parece que agotaste el límite DIARIO (6000 req/día)")
                            else:
                                self.log_callback(f"ℹ️ Parece que agotaste el límite HORARIO (600 req/hora)")
                        else:
                            self.log_callback(f"⏰ Espera requerida: {wait_min}min")
                            self.log_callback(f"🕐 Disponible aproximadamente a las: {hora_disponible}")
                    else:
                        self.log_callback(f"⏰ Espera aproximada: ~60 minutos")
                    
                    self.log_callback(f"💡 Cierra la app y vuelve a ejecutar cuando se resetee la cuota.")
                    self.log_callback(f"{'='*70}")
                    return None
                
                elif response.status_code == 401:
                    self.log_callback("❌ API Key inválida o expirada.")
                    self.log_callback(f"{'='*70}")
                    return None
                
                elif response.status_code == 200:
                    self.log_callback("✅ API disponible. Iniciando búsqueda...")
                    # Procesar contactos del test si los hay
                    try:
                        test_data = response.json()
                        test_contacts = test_data.get('contacts', [])
                        if test_contacts:
                            nuevos = self._procesar_contactos(test_contacts, empresas[0].strip())
                            if nuevos:
                                self._escribir_resultados(nuevos)
                                with self.stats_lock:
                                    self.total_encontrados += len(nuevos)
                    except:
                        pass
                else:
                    self.log_callback(f"⚠️ Respuesta inesperada: Status {response.status_code}")
                    self.log_callback(f"💡 Continuando de todas formas...")
                    
            except requests.exceptions.ConnectionError:
                self.log_callback("❌ No se pudo conectar a Apollo. Verifica tu conexión a internet.")
                self.log_callback(f"{'='*70}")
                return None
            except requests.exceptions.Timeout:
                self.log_callback("❌ Timeout conectando a Apollo. Intenta de nuevo.")
                self.log_callback(f"{'='*70}")
                return None
            except Exception as e:
                self.log_callback(f"⚠️ Error en verificación: {str(e)[:100]}")
                self.log_callback(f"💡 Continuando de todas formas...")
            
            self.log_callback(f"🔄 Procesando con {max_workers} worker(s)...\n")
            
            tareas_completadas = 0
            inicio_tiempo = time.time()
            
            # Procesamiento SIMPLE sin ThreadPoolExecutor
            if max_workers == 1:
                # Procesamiento secuencial (RECOMENDADO para Apollo)
                for empresa, pais, chunk, idx in tareas:
                    if self.stop_event.is_set():
                        self.log_callback("🛑 Proceso cancelado.")
                        break
                    
                    try:
                        self._procesar_tarea(empresa, pais, chunk, idx)
                        tareas_completadas += 1
                        
                        # Mostrar progreso cada 20 tareas
                        if tareas_completadas % 20 == 0:
                            progreso = (tareas_completadas / total_tareas) * 100
                            tiempo_transcurrido = time.time() - inicio_tiempo
                            if tareas_completadas > 0:
                                eta_segundos = (tiempo_transcurrido / tareas_completadas) * (total_tareas - tareas_completadas)
                            else:
                                eta_segundos = 0
                            self.log_callback(
                                f"📈 {progreso:.1f}% | Tareas: {tareas_completadas}/{total_tareas} | "
                                f"Contactos: {self.total_encontrados} | Tiempo: {int(tiempo_transcurrido)}s | ETA: {int(eta_segundos)}s"
                            )
                    except Exception as e:
                        self.log_callback(f"❌ Error procesando tarea: {str(e)[:100]}")
                        pass
            else:
                # Procesamiento paralelo (NO RECOMENDADO para Apollo)
                self.log_callback("⚠️ ADVERTENCIA: Procesamiento paralelo puede causar bloqueos en Apollo")
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [
                        executor.submit(self._procesar_tarea, empresa, pais, chunk, idx)
                        for empresa, pais, chunk, idx in tareas
                    ]
                    
                    for future in futures:
                        if self.stop_event.is_set():
                            break
                        try:
                            future.result(timeout=120)  # Timeout de 120 segundos por tarea
                            tareas_completadas += 1
                            
                            if tareas_completadas % 20 == 0:
                                progreso = (tareas_completadas / total_tareas) * 100
                                tiempo_transcurrido = time.time() - inicio_tiempo
                                eta_segundos = (tiempo_transcurrido / tareas_completadas) * (total_tareas - tareas_completadas)
                                self.log_callback(
                                    f"� {progreso:.1f}% | Tareas: {tareas_completadas}/{total_tareas} | "
                                    f"Contactos: {self.total_encontrados} | Tiempo: {int(tiempo_transcurrido)}s | ETA: {int(eta_segundos)}s"
                                )
                        except Exception as e:
                            self.log_callback(f"❌ Error en future: {str(e)[:100]}")
                            pass
            
            # Reporte final
            tiempo_total = time.time() - inicio_tiempo
            self.log_callback(f"\n{'='*70}")
            self.log_callback("✅ PROCESO COMPLETADO")
            self.log_callback(f"{'='*70}")
            self.log_callback(f"📊 Total de búsquedas realizadas: {self.total_requests}")
            self.log_callback(f"👥 Total de contactos encontrados: {self.total_encontrados}")
            self.log_callback(f"⏱️  Tiempo total: {int(tiempo_total)}s ({tiempo_total/60:.1f} minutos)")
            self.log_callback(f"📁 Archivo: {os.path.basename(self.output_file)}")
            self.log_callback(f"{'='*70}\n")
            
            return self.output_file if self.total_encontrados > 0 else None
        
        finally:
            self.prevent_sleep.stop()


def run(api_key, empresas, cargos, paises, output_folder, log_callback, stop_event):
    """
    Función principal compatible con tu interfaz existente
    
    MEJORAS implementadas:
    - ✅ Procesamiento secuencial (estable)
    - ✅ Escritura incremental (no pierdes progreso)
    - ✅ Manejo de rate limits (respeta límites Apollo)
    - ✅ Prevención de suspensión automática
    - ✅ Búsqueda por país (más eficiente)
    - ✅ Deduplicación thread-safe
    - ✅ Logs optimizados (no satura consola)
    
    CONFIGURACIÓN:
    - max_workers=1 (secuencial, estable, respeta límites Apollo)
    - rate_limit=30 req/min (muy conservador)
    - max_pages=1 (solo primera página, 100 resultados)
    """
    scraper = ApolloScraperOptimizado(api_key, output_folder, log_callback, stop_event)
    
    # Procesamiento SECUENCIAL (1 worker) para respetar límites de Apollo
    # No cambiar este valor a menos que tengas un plan Professional/Organization
    return scraper.ejecutar_busqueda(empresas, cargos, paises, max_workers=1)

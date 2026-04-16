"""
Configuración de Seguridad para Extractor de Contactos
Este archivo contiene constantes y funciones de seguridad recomendadas
"""

import os
import logging
from logging.handlers import RotatingFileHandler

# ============================================================
# CONFIGURACIÓN DE SEGURIDAD
# ============================================================

# Límites de archivo
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB
MAX_CSV_LINES = 10000
MAX_FIELD_LENGTH = 500

# Límites de API
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RATE_LIMIT_DELAY = 1  # segundos

# Validación
ALLOWED_FILE_EXTENSIONS = ['.csv']
MIN_API_KEY_LENGTH = 10

# ============================================================
# LOGGING SEGURO
# ============================================================

def setup_logging(log_file='extractor.log', max_bytes=10*1024*1024, backup_count=5):
    """
    Configura logging estructurado y seguro
    
    Args:
        log_file: Nombre del archivo de log
        max_bytes: Tamaño máximo antes de rotar (10MB por defecto)
        backup_count: Número de backups a mantener
    
    Returns:
        logger: Logger configurado
    """
    logger = logging.getLogger('extractor')
    logger.setLevel(logging.INFO)
    
    # Evitar duplicados
    if logger.handlers:
        return logger
    
    # Handler con rotación
    handler = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count
    )
    
    # Formato seguro (sin exponer rutas completas)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger

# ============================================================
# FUNCIONES DE VALIDACIÓN
# ============================================================

def validate_file_path(filepath, max_size=MAX_FILE_SIZE):
    """
    Valida un archivo antes de procesarlo
    
    Args:
        filepath: Ruta del archivo
        max_size: Tamaño máximo permitido
    
    Returns:
        bool: True si es válido
    
    Raises:
        ValueError: Si el archivo no es válido
    """
    # Validar que existe
    if not os.path.isfile(filepath):
        raise ValueError("Archivo no encontrado")
    
    # Validar extensión
    if not any(filepath.lower().endswith(ext) for ext in ALLOWED_FILE_EXTENSIONS):
        raise ValueError(f"Extensión no permitida. Permitidas: {ALLOWED_FILE_EXTENSIONS}")
    
    # Validar tamaño
    file_size = os.path.getsize(filepath)
    if file_size > max_size:
        raise ValueError(f"Archivo demasiado grande ({file_size / 1024 / 1024:.1f}MB, máx {max_size / 1024 / 1024:.0f}MB)")
    
    # Validar que es legible
    if not os.access(filepath, os.R_OK):
        raise ValueError("No hay permisos de lectura en el archivo")
    
    return True

def validate_api_key(api_key):
    """
    Valida una API key
    
    Args:
        api_key: La API key a validar
    
    Returns:
        bool: True si es válida
    
    Raises:
        ValueError: Si la API key no es válida
    """
    if not api_key or not isinstance(api_key, str):
        raise ValueError("API key inválida")
    
    if len(api_key) < MIN_API_KEY_LENGTH:
        raise ValueError(f"API key demasiado corta (mín {MIN_API_KEY_LENGTH} caracteres)")
    
    # Validar que no contiene caracteres sospechosos
    if any(char in api_key for char in ['\n', '\r', '\0']):
        raise ValueError("API key contiene caracteres inválidos")
    
    return True

def mask_api_key(api_key):
    """
    Enmascara una API key para logs seguros
    
    Args:
        api_key: La API key a enmascarar
    
    Returns:
        str: API key enmascarada (ej: "abc...xyz")
    """
    if not api_key or len(api_key) < 8:
        return "***"
    return f"{api_key[:4]}...{api_key[-4:]}"

def sanitize_string(value, max_length=MAX_FIELD_LENGTH):
    """
    Sanitiza un string para evitar inyecciones
    
    Args:
        value: String a sanitizar
        max_length: Longitud máxima
    
    Returns:
        str: String sanitizado
    """
    if not isinstance(value, str):
        return str(value)[:max_length]
    
    # Remover caracteres de control
    value = ''.join(char for char in value if ord(char) >= 32 or char in '\n\r\t')
    
    # Limitar longitud
    value = value[:max_length]
    
    # Remover espacios en blanco excesivos
    value = ' '.join(value.split())
    
    return value

# ============================================================
# FUNCIONES DE SEGURIDAD DE DISCO
# ============================================================

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

def safe_file_write(filepath, content, mode='w', encoding='utf-8'):
    """
    Escribe a archivo de forma segura
    
    Args:
        filepath: Ruta del archivo
        content: Contenido a escribir
        mode: Modo de apertura
        encoding: Codificación
    
    Returns:
        bool: True si fue exitoso
    """
    try:
        # Verificar espacio
        check_disk_space(os.path.dirname(filepath) or '.')
        
        # Escribir a archivo temporal primero
        temp_file = f"{filepath}.tmp"
        with open(temp_file, mode=mode, encoding=encoding) as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        
        # Renombrar si fue exitoso
        if os.path.exists(filepath):
            os.remove(filepath)
        os.rename(temp_file, filepath)
        
        return True
    
    except Exception as e:
        # Limpiar archivo temporal si existe
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except:
                pass
        raise

# ============================================================
# FUNCIONES DE RATE LIMITING
# ============================================================

import time
from datetime import datetime, timedelta

class RateLimiter:
    """
    Limitador de velocidad para respetar límites de API
    """
    def __init__(self, requests_per_minute=60):
        self.requests_per_minute = requests_per_minute
        self.min_interval = 60 / requests_per_minute
        self.last_request = None
    
    def wait(self):
        """Espera el tiempo necesario para respetar rate limit"""
        if self.last_request:
            elapsed = (datetime.now() - self.last_request).total_seconds()
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
        self.last_request = datetime.now()

# ============================================================
# FUNCIONES DE VALIDACIÓN DE RESPUESTA
# ============================================================

def validate_api_response(response_data, max_size_kb=1000):
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
        raise ValueError(f"Respuesta demasiado grande ({size_kb:.1f}KB, máx {max_size_kb}KB)")
    
    return True

# ============================================================
# EJEMPLO DE USO
# ============================================================

if __name__ == "__main__":
    # Configurar logging
    logger = setup_logging()
    logger.info("Sistema de seguridad inicializado")
    
    # Ejemplo de validación
    try:
        validate_api_key("test_key_1234567890")
        print("✅ API key válida")
    except ValueError as e:
        print(f"❌ {e}")
    
    # Ejemplo de enmascaramiento
    api_key = "sk_live_1234567890abcdefghij"
    print(f"API key original: {api_key}")
    print(f"API key enmascarada: {mask_api_key(api_key)}")

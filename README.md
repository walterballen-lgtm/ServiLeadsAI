# 🚀 Extractor de Contactos v3.8 - Bloqueos Resueltos

## 📋 Descripción

Herramienta de extracción de contactos desde múltiples fuentes (Apollo, Lusha) con seguridad mejorada, validación completa, rate limiting inteligente y optimizaciones para evitar bloqueos.

---

## ✅ Bloqueos Resueltos - v3.8 (FINAL)

Se implementaron correcciones definitivas para evitar bloqueos:

### Cambios Implementados
1. ✅ **Rate Limiter Reducido**: De 300 a 5 req/min (respeta límite de Apollo)
2. ✅ **Rate Limiter Aplicado**: Se aplica en cada request
3. ✅ **Timeout Aumentado**: De 10 a 15 segundos
4. ✅ **Backoff Exponencial**: Para errores 429 (60s → 120s → 240s)
5. ✅ **Manejo de Errores**: Específico para 401, 403, 500
6. ✅ **Logging Detallado**: Cada request visible en tiempo real
7. ✅ **Timeout en Futures**: 120 segundos para evitar bloqueos indefinidos
8. ✅ **ETA Mejorado**: Cálculo preciso del tiempo restante

**Resultado:** Sistema estable que respeta límites de Apollo y NO se bloquea

Ver `CORRECCION_BLOQUEOS_v3.8.md` para detalles técnicos.

---

## ✨ Características Principales

### 1. **Rate Limiting Inteligente** 📊
- 5 req/min (12 segundos entre requests)
- Respeta límite de Apollo (400 req/hora)
- Backoff exponencial para 429
- Sin bloqueos ni deadlocks

### 2. **Seguridad Mejorada** 🔐
- API keys enmascaradas en logs
- Validación de respuestas de API
- Verificación de espacio en disco
- Rate limiting automático

### 3. **Optimizaciones de Rendimiento** ⚡
- Procesamiento secuencial (estable)
- Timeout robusto (15 segundos)
- Manejo de errores específicos
- Logging detallado para diagnóstico

### 4. **Manejo de Errores** 🛡️
- Retry logic con backoff exponencial
- Logs detallados para debugging
- Detección de problemas de conexión
- Manejo de timeouts

---

## ✅ Verificación

### Ejecutar Pruebas de Seguridad
```bash
python TEST_SEGURIDAD.py
```

**Resultado esperado:** 6/6 pruebas pasadas ✅

### Ejecutar Aplicación
```bash
python app_principal.py
```

### Verificar Conexión a Apollo
```bash
python TEST_APOLLO_CONNECTION.py <TU_API_KEY>
```

---

## 📁 Estructura del Proyecto

### Archivos Funcionales
- `app_principal.py` - Interfaz gráfica principal
- `apollo_script.py` - Extracción de Apollo (optimizado v3.8)
- `lusha_script.py` - Extracción de Lusha
- `apollo_org.py` - Extracción de organizaciones Apollo
- `lusha_org.py` - Extracción de organizaciones Lusha

### Módulos de Seguridad
- `SECURITY_CONFIG.py` - Funciones de seguridad reutilizables

### Pruebas
- `TEST_SEGURIDAD.py` - Suite de 6 pruebas
- `TEST_APOLLO_CONNECTION.py` - Test de conexión a Apollo

### Datos
- `Archivos/` - Archivos de entrada (CSV)
- `Resultados/` - Archivos de salida (CSV)

---

## 🔐 Características de Seguridad

### Validación de Entrada
- ✅ Validación de archivos CSV
- ✅ Límite de 10MB por archivo
- ✅ Límite de 10,000 líneas por archivo
- ✅ Sanitización de valores

### Protección de Credenciales
- ✅ API keys enmascaradas en logs
- ✅ Validación de API keys
- ✅ Logs seguros sin exposición de datos

### Validación de API
- ✅ Validación de respuestas
- ✅ Manejo de errores mejorado
- ✅ Timeout robusto (15s)

### Gestión de Recursos
- ✅ Verificación de espacio en disco
- ✅ Rate limiting automático (5 req/min)
- ✅ Límites de concurrencia

---

## 🚀 Uso Rápido

### 1. Verificar que todo funciona
```bash
python TEST_SEGURIDAD.py
```

### 2. Ejecutar la aplicación
```bash
python app_principal.py
```

### 3. Usar la interfaz
- Ingresa tus API keys
- Selecciona archivos CSV
- Elige países
- Haz clic en el botón de proceso

---

## 🛠️ Solución de Problemas

### Sistema se queda bloqueado
```bash
python TEST_APOLLO_CONNECTION.py <TU_API_KEY>
```

Ver `CORRECCION_BLOQUEOS_v3.8.md` para más detalles.

### "ModuleNotFoundError: No module named 'customtkinter'"
```bash
pip install customtkinter
```

### "TEST_SEGURIDAD.py falla"
Verifica que todos los archivos están en el mismo directorio

### "Espacio en disco insuficiente"
Libera espacio en disco (mínimo 100MB)

### "Rate limit alcanzado"
El sistema espera automáticamente (60s → 120s → 240s)

---

## 📚 Documentación

### Documentos Principales
- `CORRECCION_BLOQUEOS_v3.8.md` - Correcciones de bloqueos (v3.8)
- `CAMBIOS_v3.8.md` - Resumen de cambios
- `INSTRUCCIONES_USO.md` - Guía completa de uso

### Pruebas
- `TEST_SEGURIDAD.py` - Suite de pruebas
- `TEST_APOLLO_CONNECTION.py` - Test de conexión

---

## ✨ Características

✅ **Seguro** - API keys enmascaradas, validación completa
✅ **Rápido** - Rate limiting inteligente, sin bloqueos
✅ **Confiable** - Manejo de errores mejorado, logs detallados
✅ **Estable** - Respeta límites de Apollo, procesamiento secuencial

---

## 📈 Configuración

### Configuración Actual (v3.8 - Estable)
```python
rate_limit = 5 req/min       # 12 segundos entre requests
timeout = 15 segundos        # Por request
max_workers = 1              # Procesamiento secuencial
backoff_429 = exponencial    # 60s → 120s → 240s
timeout_future = 120s        # Por future
```

**Nota**: Esta configuración respeta los límites oficiales de Apollo (400 req/hora) y garantiza estabilidad total.

### Si Necesitas Más Velocidad
```python
rate_limit = 10 req/min  # Aumentar a 10 (si tu plan lo permite)
```

### Si Aún Hay Problemas
```python
rate_limit = 3 req/min   # Reducir a 3 (más conservador)
```

---

## 📞 Soporte

Para verificar que todo funciona:
```bash
python TEST_SEGURIDAD.py
```

Para diagnosticar bloqueos:
```bash
python TEST_APOLLO_CONNECTION.py <TU_API_KEY>
```

---

**Última actualización:** Marzo 2026
**Versión:** 3.8
**Estado:** ✅ Bloqueos Resueltos + Estable + Listo para Producción


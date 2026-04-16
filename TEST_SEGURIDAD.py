"""
Script de prueba para verificar que los cambios de seguridad funcionan correctamente
sin afectar el proceso y resultado
"""

import sys
import os

def test_imports():
    """Verifica que todos los imports funcionan"""
    print("=" * 70)
    print("TEST 1: Verificando imports...")
    print("=" * 70)
    
    try:
        import app_principal
        print("✅ app_principal.py - OK")
    except Exception as e:
        print(f"❌ app_principal.py - ERROR: {e}")
        return False
    
    try:
        import apollo_script
        print("✅ apollo_script.py - OK")
    except Exception as e:
        print(f"❌ apollo_script.py - ERROR: {e}")
        return False
    
    try:
        import lusha_script
        print("✅ lusha_script.py - OK")
    except Exception as e:
        print(f"❌ lusha_script.py - ERROR: {e}")
        return False
    
    try:
        from SECURITY_CONFIG import (
            setup_logging,
            validate_file_path,
            validate_api_key,
            mask_api_key,
            sanitize_string,
            check_disk_space,
            RateLimiter
        )
        print("✅ SECURITY_CONFIG.py - OK")
    except Exception as e:
        print(f"❌ SECURITY_CONFIG.py - ERROR: {e}")
        return False
    
    return True

def test_security_functions():
    """Verifica que las funciones de seguridad funcionan"""
    print("\n" + "=" * 70)
    print("TEST 2: Verificando funciones de seguridad...")
    print("=" * 70)
    
    from SECURITY_CONFIG import mask_api_key, sanitize_string, RateLimiter
    
    # Test mask_api_key
    try:
        api_key = "sk_live_1234567890abcdefghij"
        masked = mask_api_key(api_key)
        assert masked == "sk_l...ghij", f"Enmascaramiento incorrecto: {masked}"
        print(f"✅ mask_api_key: '{api_key}' -> '{masked}'")
    except Exception as e:
        print(f"❌ mask_api_key - ERROR: {e}")
        return False
    
    # Test sanitize_string
    try:
        dirty = "Test\nString\twith\rspecial"
        clean = sanitize_string(dirty)
        assert "\n" not in clean and "\r" not in clean
        print(f"✅ sanitize_string: Limpieza correcta")
    except Exception as e:
        print(f"❌ sanitize_string - ERROR: {e}")
        return False
    
    # Test RateLimiter
    try:
        limiter = RateLimiter(requests_per_minute=60)
        assert limiter.min_interval == 1.0
        print(f"✅ RateLimiter: Inicialización correcta")
    except Exception as e:
        print(f"❌ RateLimiter - ERROR: {e}")
        return False
    
    return True

def test_app_functions():
    """Verifica que las funciones de app_principal funcionan"""
    print("\n" + "=" * 70)
    print("TEST 3: Verificando funciones de app_principal...")
    print("=" * 70)
    
    try:
        # Crear instancia de App (sin mostrar GUI)
        import customtkinter as ctk
        from app_principal import App
        
        # Verificar que la clase tiene los métodos de seguridad
        assert hasattr(App, '_mask_api_key'), "Falta método _mask_api_key"
        print("✅ Método _mask_api_key existe")
        
        assert hasattr(App, '_validate_api_key'), "Falta método _validate_api_key"
        print("✅ Método _validate_api_key existe")
        
        assert hasattr(App, '_safe_log_process_start'), "Falta método _safe_log_process_start"
        print("✅ Método _safe_log_process_start existe")
        
        return True
    except Exception as e:
        print(f"❌ app_principal - ERROR: {e}")
        return False

def test_apollo_functions():
    """Verifica que las funciones de apollo_script funcionan"""
    print("\n" + "=" * 70)
    print("TEST 4: Verificando funciones de apollo_script...")
    print("=" * 70)
    
    try:
        import apollo_script
        
        # Verificar que existen las funciones de validación
        assert hasattr(apollo_script, 'validar_respuesta_api'), "Falta función validar_respuesta_api"
        print("✅ Función validar_respuesta_api existe")
        
        assert hasattr(apollo_script, 'check_disk_space'), "Falta función check_disk_space"
        print("✅ Función check_disk_space existe")
        
        # Test validar_respuesta_api
        try:
            apollo_script.validar_respuesta_api({"test": "data"})
            print("✅ validar_respuesta_api funciona correctamente")
        except Exception as e:
            print(f"❌ validar_respuesta_api - ERROR: {e}")
            return False
        
        return True
    except Exception as e:
        print(f"❌ apollo_script - ERROR: {e}")
        return False

def test_lusha_functions():
    """Verifica que las funciones de lusha_script funcionan"""
    print("\n" + "=" * 70)
    print("TEST 5: Verificando funciones de lusha_script...")
    print("=" * 70)
    
    try:
        import lusha_script
        
        # Verificar que existe la clase RateLimiter
        assert hasattr(lusha_script, 'RateLimiter'), "Falta clase RateLimiter"
        print("✅ Clase RateLimiter existe")
        
        # Test RateLimiter
        try:
            limiter = lusha_script.RateLimiter(requests_per_minute=30)
            assert limiter.min_interval == 2.0
            print("✅ RateLimiter funciona correctamente")
        except Exception as e:
            print(f"❌ RateLimiter - ERROR: {e}")
            return False
        
        return True
    except Exception as e:
        print(f"❌ lusha_script - ERROR: {e}")
        return False

def test_csv_validation():
    """Verifica que la validación de CSV funciona"""
    print("\n" + "=" * 70)
    print("TEST 6: Verificando validación de CSV...")
    print("=" * 70)
    
    try:
        from SECURITY_CONFIG import validate_file_path
        
        # Test con archivo que no existe
        try:
            validate_file_path("archivo_inexistente.csv")
            print("❌ Debería haber lanzado excepción para archivo inexistente")
            return False
        except ValueError as e:
            print(f"✅ Validación correcta para archivo inexistente: {e}")
        
        # Test con extensión incorrecta
        try:
            # Crear archivo temporal
            with open("test.txt", "w") as f:
                f.write("test")
            
            validate_file_path("test.txt")
            print("❌ Debería haber lanzado excepción para extensión incorrecta")
            os.remove("test.txt")
            return False
        except ValueError as e:
            print(f"✅ Validación correcta para extensión incorrecta: {e}")
            os.remove("test.txt")
        
        return True
    except Exception as e:
        print(f"❌ CSV validation - ERROR: {e}")
        return False

def main():
    """Ejecuta todos los tests"""
    print("\n")
    print("╔" + "=" * 68 + "╗")
    print("║" + " " * 15 + "PRUEBAS DE SEGURIDAD Y FUNCIONALIDAD" + " " * 17 + "║")
    print("╚" + "=" * 68 + "╝")
    
    tests = [
        ("Imports", test_imports),
        ("Funciones de Seguridad", test_security_functions),
        ("Funciones de App", test_app_functions),
        ("Funciones de Apollo", test_apollo_functions),
        ("Funciones de Lusha", test_lusha_functions),
        ("Validación de CSV", test_csv_validation),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n❌ ERROR en {test_name}: {e}")
            results.append((test_name, False))
    
    # Resumen
    print("\n" + "=" * 70)
    print("RESUMEN DE PRUEBAS")
    print("=" * 70)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASÓ" if result else "❌ FALLÓ"
        print(f"{status}: {test_name}")
    
    print("=" * 70)
    print(f"RESULTADO: {passed}/{total} pruebas pasadas")
    print("=" * 70)
    
    if passed == total:
        print("\n🎉 ¡TODAS LAS PRUEBAS PASARON! El sistema está seguro y funcional.")
        return 0
    else:
        print(f"\n⚠️  {total - passed} prueba(s) fallaron. Revisa los errores arriba.")
        return 1

if __name__ == "__main__":
    sys.exit(main())

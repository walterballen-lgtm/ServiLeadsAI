"""
Test simple para verificar conexión a Apollo
"""
import requests
import json
import sys

def test_apollo_connection(api_key):
    """Test simple de conexión a Apollo"""
    
    print("=" * 70)
    print("TEST DE CONEXIÓN A APOLLO")
    print("=" * 70)
    
    url = "https://api.apollo.io/api/v1/contacts/search"
    headers = {
        'Cache-Control': 'no-cache',
        'Content-Type': 'application/json',
        'accept': 'application/json',
        'x-api-key': api_key
    }
    
    payload = {
        "q_organization_name": "Google",
        "organization_locations": ["United States"],
        "person_titles": ["CEO"],
        "page": 1,
        "per_page": 10
    }
    
    print(f"\n📡 Enviando request a Apollo...")
    print(f"URL: {url}")
    print(f"API Key: {api_key[:10]}...{api_key[-4:]}")
    print(f"Payload: {json.dumps(payload, indent=2)}")
    
    try:
        print(f"\n⏳ Esperando respuesta (timeout: 5s)...")
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=(5, 10)
        )
        
        print(f"\n✓ Response recibida!")
        print(f"Status Code: {response.status_code}")
        print(f"Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"\n✅ ÉXITO!")
            print(f"Contactos encontrados: {len(data.get('contacts', []))}")
            print(f"Respuesta: {json.dumps(data, indent=2)[:500]}...")
            return True
        
        elif response.status_code == 401:
            print(f"\n❌ ERROR 401: API Key inválida o expirada")
            print(f"Respuesta: {response.text}")
            return False
        
        elif response.status_code == 429:
            print(f"\n⚠️ ERROR 429: Rate limit alcanzado")
            print(f"Respuesta: {response.text}")
            return False
        
        else:
            print(f"\n⚠️ ERROR {response.status_code}")
            print(f"Respuesta: {response.text}")
            return False
    
    except requests.exceptions.Timeout:
        print(f"\n❌ TIMEOUT: La request tardó más de 5 segundos")
        print(f"Posibles causas:")
        print(f"  - Conexión de red lenta")
        print(f"  - Servidor de Apollo no responde")
        print(f"  - Firewall bloqueando la conexión")
        return False
    
    except requests.exceptions.ConnectionError as e:
        print(f"\n❌ ERROR DE CONEXIÓN: {str(e)}")
        print(f"Posibles causas:")
        print(f"  - Sin conexión a internet")
        print(f"  - Firewall bloqueando apollo.io")
        print(f"  - DNS no resuelve apollo.io")
        return False
    
    except Exception as e:
        print(f"\n❌ ERROR INESPERADO: {str(e)}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python TEST_APOLLO_CONNECTION.py <API_KEY>")
        print("\nEjemplo:")
        print("  python TEST_APOLLO_CONNECTION.py gM8s...RGuw")
        sys.exit(1)
    
    api_key = sys.argv[1]
    success = test_apollo_connection(api_key)
    
    print("\n" + "=" * 70)
    if success:
        print("✅ Conexión a Apollo OK - El problema está en otro lado")
    else:
        print("❌ Conexión a Apollo FALLIDA - Revisar API Key y conexión de red")
    print("=" * 70)

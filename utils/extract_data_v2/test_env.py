#!/usr/bin/env python3

import os
from pathlib import Path

# Intentar cargar dotenv
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent / '.env'
    print(f"Looking for .env at: {env_path}")
    print(f".env exists: {env_path.exists()}")
    
    if env_path.exists():
        load_dotenv(env_path)
        print("✅ .env loaded successfully")
    else:
        print("❌ .env file not found")
        
except ImportError:
    print("❌ python-dotenv not installed")
    print("Install with: pip install python-dotenv")

# Verificar variables clave
test_vars = [
    'MAX_THREADS', 'CHUNK_SIZE', 'PROJECT_NAME', 
    'TEAM', 'DATA_SOURCE', 'AWS_PROFILE'
]

print("\n=== Environment Variables ===")
for var in test_vars:
    value = os.getenv(var, 'NOT_SET')
    print(f"{var}: {value}")

# Verificar si settings carga correctamente
try:
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from config.settings import settings
    
    print("\n=== Settings Object ===")
    config = settings.get_all()
    for var in test_vars:
        value = config.get(var, 'NOT_SET')
        print(f"{var}: {value}")
        
except Exception as e:
    print(f"❌ Error loading settings: {e}")
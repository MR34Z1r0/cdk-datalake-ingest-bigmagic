#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para configurar y verificar la conexión AWS
"""

import boto3
import os
import sys
from botocore.exceptions import ProfileNotFound, NoCredentialsError

def test_aws_configuration():
    """Verificar configuración de AWS"""
    print("🔍 Verificando configuración de AWS...")
    
    # Intentar obtener perfil configurado
    profile_name = os.getenv('AWS_PROFILE')
    region_name = os.getenv('REGION', 'us-east-1')
    
    try:
        if profile_name:
            print(f"📋 Usando perfil AWS: {profile_name}")
            print(f"🌍 Región: {region_name}")
            
            # Setup session with profile
            boto3.setup_default_session(
                profile_name=profile_name,
                region_name=region_name
            )
            
            # Test connection
            sts = boto3.client('sts')
            identity = sts.get_caller_identity()
            
            print("✅ Conexión exitosa!")
            print(f"   Account: {identity.get('Account')}")
            print(f"   UserId: {identity.get('UserId')}")
            print(f"   Arn: {identity.get('Arn')}")
            
        else:
            print("📋 Usando credenciales por defecto (sin perfil)")
            
            # Test with default credentials
            sts = boto3.client('sts', region_name=region_name)
            identity = sts.get_caller_identity()
            
            print("✅ Conexión exitosa!")
            print(f"   Account: {identity.get('Account')}")
            print(f"   UserId: {identity.get('UserId')}")
        
        # Test S3 access
        s3_bucket = os.getenv('S3_RAW_BUCKET')
        if s3_bucket:
            print(f"\n📦 Verificando acceso a S3: {s3_bucket}")
            s3 = boto3.client('s3')
            try:
                s3.head_bucket(Bucket=s3_bucket)
                print("✅ Acceso a S3 confirmado")
            except Exception as e:
                print(f"⚠️  Advertencia - No se puede acceder al bucket S3: {e}")
        
        # Test DynamoDB access
        dynamo_table = os.getenv('DYNAMO_LOGS_TABLE')
        if dynamo_table:
            print(f"\n📊 Verificando acceso a DynamoDB: {dynamo_table}")
            dynamo = boto3.client('dynamodb')
            try:
                dynamo.describe_table(TableName=dynamo_table)
                print("✅ Acceso a DynamoDB confirmado")
            except Exception as e:
                print(f"⚠️  Advertencia - No se puede acceder a la tabla DynamoDB: {e}")
        
        return True
        
    except ProfileNotFound as e:
        print(f"❌ Error: Perfil AWS no encontrado: {e}")
        print("💡 Solución: Ejecuta 'aws configure --profile tu_perfil' para configurarlo")
        return False
        
    except NoCredentialsError:
        print("❌ Error: No se encontraron credenciales AWS")
        print("💡 Solución: Configura AWS_PROFILE o AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY")
        return False
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def show_available_profiles():
    """Mostrar perfiles disponibles"""
    try:
        session = boto3.Session()
        profiles = session.available_profiles
        
        print("\n📋 Perfiles AWS disponibles:")
        for profile in profiles:
            print(f"   • {profile}")
            
        if not profiles:
            print("   (No hay perfiles configurados)")
            
    except Exception as e:
        print(f"Error obteniendo perfiles: {e}")

def main():
    print("🚀 Setup AWS para Sistema de Extracción de Datos")
    print("=" * 50)
    
    # Mostrar perfiles disponibles
    show_available_profiles()
    
    # Verificar configuración actual
    success = test_aws_configuration()
    
    if success:
        print("\n✅ AWS configurado correctamente!")
        print("🎉 Ya puedes ejecutar el sistema de extracción")
    else:
        print("\n❌ Hay problemas con la configuración AWS")
        print("📖 Revisa la documentación para configurar correctamente")
        return 1
    
    return 0

if __name__ == '__main__':
    sys.exit(main())
#!/usr/bin/env python3

import os
import sys
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import settings
from models.database_config import DatabaseConfig
from extract.extractors.sql_server_extractor import SQLServerExtractor

def debug_table_data():
    """Debug using the REAL credentials from CSV"""
    
    # Use the SAME configuration that the main process uses
    secret_name = "dev/datalake/apdayc/bigmagic"
    
    db_config = DatabaseConfig(
        endpoint_name="PEBDDATA2",
        db_type="mssql",
        server="172.16.0.33",        # ← Correcto del CSV
        database="BDDATA",           # ← Correcto del CSV
        username="usr_datalake",     # ← Correcto del CSV
        secret_key="password",
        port=1433,
        secret_name=secret_name
    )
    
    # Create extractor
    extractor = SQLServerExtractor(db_config)
    
    try:
        print("🔍 Connecting to database...")
        extractor.connect()
        
        print("\n=== VERIFICANDO DATOS EN LA TABLA ===")
        
        # 1. Count total records
        count_query = """
        SELECT COUNT(*) as total_records 
        FROM dbo.tclicg2f t2 
        inner join dbo.tclicg1f(nolock) t on t.compania = t2.ciaabono 
          and t.sucursal = t2.sucursal 
          and t.transaccio = t2.transabono 
          and t.nroserie = t2.nroserabon 
          and t.nrodoc = t2.nrodocabon 
        WHERE t.fecha <> 0 
          AND (t2.ciaabono in (select compania from dbo.mcompa1f b where b.flgbi = 'a'))
        """
        
        df_count = extractor.execute_query(count_query)
        total_records = df_count.iloc[0]['total_records']
        print(f"📊 Total records with filters: {total_records:,}")
        
        if total_records == 0:
            print("❌ NO HAY DATOS CON ESOS FILTROS")
            
            # Check without filters
            simple_count = "SELECT COUNT(*) as total FROM dbo.tclicg2f"
            df_simple = extractor.execute_query(simple_count)
            print(f"📊 Total records WITHOUT filters: {df_simple.iloc[0]['total']:,}")
            
            # Check companies
            company_query = """
            SELECT DISTINCT t2.ciaabono, COUNT(*) as records
            FROM dbo.tclicg2f t2 
            GROUP BY t2.ciaabono
            ORDER BY records DESC
            """
            df_companies = extractor.execute_query(company_query)
            print(f"\n📋 Companies in tclicg2f:")
            for _, row in df_companies.head(10).iterrows():
                print(f"   Company: {row['ciaabono']} - Records: {row['records']:,}")
            
            # Check filter table
            filter_query = "SELECT compania, flgbi FROM dbo.mcompa1f WHERE flgbi = 'a'"
            try:
                df_filter = extractor.execute_query(filter_query)
                print(f"\n📋 Companies in mcompa1f with flgbi='a':")
                for _, row in df_filter.head(10).iterrows():
                    print(f"   Company: {row['compania']}")
            except Exception as e:
                print(f"❌ Error checking mcompa1f: {e}")
            
        else:
            # Continue with the analysis we had before
            # 2. Min/Max dates
            min_max_query = """
            SELECT 
                MIN(t.fecha) as min_fecha,
                MAX(t.fecha) as max_fecha
            FROM dbo.tclicg2f t2 
            inner join dbo.tclicg1f(nolock) t on t.compania = t2.ciaabono 
              and t.sucursal = t2.sucursal 
              and t.transaccio = t2.transabono 
              and t.nroserie = t2.nroserabon 
              and t.nrodoc = t2.nrodocabon 
            WHERE t.fecha <> 0 
              AND (t2.ciaabono in (select compania from dbo.mcompa1f b where b.flgbi = 'a'))
            """
            
            df_dates = extractor.execute_query(min_max_query)
            min_fecha = df_dates.iloc[0]['min_fecha']
            max_fecha = df_dates.iloc[0]['max_fecha']
            print(f"📅 Date range: {min_fecha} to {max_fecha}")
            
            # 3. Check the specific ranges that failed
            range_queries = [
                ("Range 1", "t.fecha >= 735669 AND t.fecha < 737594"),
                ("Range 2", "t.fecha >= 737594 AND t.fecha < 739520")
            ]
            
            for range_name, range_condition in range_queries:
                range_query = f"""
                SELECT COUNT(*) as count 
                FROM dbo.tclicg2f t2 
                inner join dbo.tclicg1f(nolock) t on t.compania = t2.ciaabono 
                  and t.sucursal = t2.sucursal 
                  and t.transaccio = t2.transabono 
                  and t.nroserie = t2.nroserabon 
                  and t.nrodoc = t2.nrodocabon 
                WHERE {range_condition}
                  AND (t2.ciaabono in (select compania from dbo.mcompa1f b where b.flgbi = 'a'))
                """
                
                df_range = extractor.execute_query(range_query)
                count = df_range.iloc[0]['count']
                print(f"📈 {range_name} ({range_condition}): {count:,} records")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        extractor.close()

if __name__ == "__main__":
    debug_table_data()
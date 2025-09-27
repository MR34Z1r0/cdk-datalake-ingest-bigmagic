#!/usr/bin/env python3

import os
import sys
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import settings
from models.database_config import DatabaseConfig
from extract.extractors.sql_server_extractor import SQLServerExtractor

def test_exact_query():
    """Test the exact query that the orchestrator should be running"""
    
    secret_name = "dev/datalake/apdayc/bigmagic"
    
    db_config = DatabaseConfig(
        endpoint_name="PEBDDATA2",
        db_type="mssql",
        server="172.16.0.33",
        database="BDDATA",
        username="usr_datalake",
        secret_key="password",
        port=1433,
        secret_name=secret_name
    )
    
    extractor = SQLServerExtractor(db_config)
    
    try:
        print("🔍 Connecting...")
        extractor.connect()
        
        # Esta es la query exacta que el orchestrator debería estar construyendo
        # Basada en el CSV que vimos antes
        exact_query = """
        SELECT 
            ltrim(rtrim(t2.ciaabono))+'|'+ltrim(rtrim(t2.sucursal))+'|'+ltrim(rtrim(t2.transabono))+'|'+ltrim(rtrim(t2.nroserabon))+'|'+ltrim(rtrim(t2.nrodocabon)) as id,
            dbo.func_cas_todatetime(t2.fecultmod, t2.horultimod) lastmodifydate, 
            t2.ciaabono, t2.sucursal, t2.transabono, t2.nroserabon, t2.nrodocabon, 
            t2.ciacargo, t2.succargo, t2.areacargo, t2.cajacargo, t2.transcargo, 
            t2.tipomovbco, t2.nrosercarg, t2.nrodocargo, t2.fecha, t2.fecvcmto, 
            t2.fechahora, t2.importe, t2.sdodoc, t2.imppagado, t2.moneda, t2.tcadolar, 
            t2.tcsoladol, t2.fecpagodoc, cast(t2.cliente as varchar(20)) cliente, 
            t2.estado, t2.feccrea, t2.horcrea, t2.usucrea, t2.fecultmod, t2.horultimod, 
            t2.ultusumod, t2.feccontab, t2.nroregistr, cast(t2.codctabco as varchar(20)) codctabco, 
            cast(t2.cobrador as varchar(20)) cobrador, t2.fecconfirma, t2.idcobext, 
            t2.compania, cast(t2.idlicencia as varchar(20)) idlicencia, 
            cast(t2.idseclicencia as varchar(20)) idseclicencia, cast(t2.idcuota as varchar(20)) idcuota
        FROM dbo.tclicg2f t2 
        inner join dbo.tclicg1f(nolock) t on t.compania = t2.ciaabono 
          and t.sucursal = t2.sucursal 
          and t.transaccio = t2.transabono 
          and t.nroserie = t2.nroserabon 
          and t.nrodoc = t2.nrodocabon 
        WHERE t.fecha >= 735669 AND t.fecha < 739520 
          AND (t2.ciaabono in (select compania from dbo.mcompa1f b where b.flgbi = 'a'))
        ORDER BY t.fecha 
        OFFSET 0 ROWS FETCH NEXT 10000 ROWS ONLY
        """
        
        print("🔍 Testing exact query with chunking...")
        print("Query:")
        print(exact_query)
        print("\n" + "="*80 + "\n")
        
        df = extractor.execute_query(exact_query)
        print(f"✅ Query returned {len(df)} rows")
        
        if len(df) > 0:
            print("📋 Column info:")
            print(f"Columns: {list(df.columns)}")
            print(f"Data types: {df.dtypes.to_dict()}")
            print("\n📋 First 3 rows:")
            print(df.head(3).to_string(index=False))
        else:
            print("❌ Query returned 0 rows!")
            
            # Try without OFFSET/FETCH
            simple_query = exact_query.replace("ORDER BY t.fecha OFFSET 0 ROWS FETCH NEXT 10000 ROWS ONLY", "")
            simple_query = f"SELECT TOP 10 * FROM ({simple_query}) subq"
            
            print("\n🔍 Trying simplified version...")
            df_simple = extractor.execute_query(simple_query)
            print(f"Simplified query returned {len(df_simple)} rows")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        extractor.close()

if __name__ == "__main__":
    test_exact_query()
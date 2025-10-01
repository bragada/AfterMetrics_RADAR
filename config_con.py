import pandas as pd
from sqlalchemy import create_engine, text
import mysql.connector
from typing import Optional
import os
from dotenv import load_dotenv

# Configurar pandas
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

def conectar_postgresql(query: str) -> pd.DataFrame:

    try:
        # Configurar pandas
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)

        # Obter URL do PostgreSQL das variáveis de ambiente
        db_url = os.getenv('URL_FRAGADB')
        
        if not db_url:
            raise ValueError("Variável de ambiente POSTGRES_URL não encontrada. Verifique o arquivo .env")
        
        # Criar engine do SQLAlchemy
        engine = create_engine(db_url)
        
        # Executar query e retornar DataFrame
        with engine.connect() as conn:
            print("Conexão PostgreSQL realizada com sucesso!")
            df = pd.read_sql_query(text(query), conn)
            print(f"Query executada com sucesso! Retornou {len(df)} linhas.")
            return df
            
    except Exception as e:
        print(f"Erro na conexão PostgreSQL: {e}")
        raise

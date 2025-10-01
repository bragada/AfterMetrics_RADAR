import os
import json
import pandas as pd
from datetime import datetime
import glob
import duckdb
from dotenv import load_dotenv
import sys
import io

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload



# Google oauth2
SCOPES = ['https://www.googleapis.com/auth/drive.file']

def authenticate():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    
    return build('drive', 'v3', credentials=creds)

db_url = os.getenv('URL_FRAGADB')
engine1 = create_engine(db_url)


def extrair_grupos_produto(engine):
    try:
        with engine.connect() as conn:

            gp_produto_autozone = [
                "Amortecedor de suspensão", "Atuador de embreagem", "Bandeja de suspensão", "Bobina de ignição",
                "Bomba D''água", "Bomba de combustível", "Bucha da bandeja", "Cabo de ignição", "Cilindro auxiliar de embreagem",
                "Cilindro mestre de embreagem", "Coxim do motor", "Disco de freio", "Filtro de ar", "Filtro de cabine", 
                "Filtro de combustível", "Filtro de óleo", "Junta homocinética", "Junta homocinética deslizante", "Kit de reparo do amortecedor",
                "Kit de correia", "Kit de embreagem", "Pastilha de Freio", "Pivô de suspensão", "Radiador", "Rolamento de roda",
                "Terminal axial", "Terminal de direção", "Válvula termostática", "Vela de ignição"
                ]
                
            query_grupos_produto = text(f"""
            select 
                gp."Id",
                gp."Nome"
            from 
                "GrupoProduto" gp
            WHERE gp."Nome" IN ({",".join(f"'{nome}'" for nome in gp_produto_autozone)})
            """)
            result = conn.execute(query_grupos_produto)

            rows = result.fetchall()

            # Convertendo o resultado para um dicionário
            dict_grupos_produto = {row[1]: row[0] for row in rows}

            print("Extração de grupos de produto realizada!")
            return dict_grupos_produto
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        return None

extrair_grupos_produto(engine1)

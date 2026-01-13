#!/usr/bin/env python3
"""
Script de Teste de Download do Acervo SPU
Baseado em: documentacao/download-arquivos-acervo-cgdai.ipynb

Este script realiza o download de um arquivo de teste para validar a conectividade
e as credenciais de acesso à API.
"""

import requests
import sys
from pathlib import Path

# --- Configurações (Extraídas do Notebook) ---
BASE_URL = "https://spuservices.spu.gestao.gov.br"
API_KEY = "98a7b6c5-d4e3-f2a1-b098-7c6d5e4f3a2b"
BUCKET = "arquivos-acervo"

# Arquivo padrão conhecido que existe no bucket
DEFAULT_FILE = "1550846346565-null-8.tif-paraPDF.pdf"

def baixar_arquivo(nome_arquivo):
    """Realiza o download do arquivo especificado."""
    
    url = f"{BASE_URL}/acervo/arquivo/{BUCKET}/{nome_arquivo}"
    headers = {
        "X-API-Key": API_KEY
    }
    
    output_path = Path(f"downloaded_{nome_arquivo}")
    
    print("=" * 60)
    print(f"🚀 Iniciando download via Python")
    print(f"➡️  URL: {url}")
    print(f"📄 Arquivo: {nome_arquivo}")
    print("-" * 60)

    try:
        # stream=True é importante para arquivos grandes para não carregar tudo na RAM
        response = requests.get(url, headers=headers, stream=True)
        
        print(f"📡 Status HTTP: {response.status_code}")
        
        if response.status_code == 200:
            total_size = int(response.headers.get('content-length', 0))
            print(f"📦 Tamanho Total: {total_size / 1024 / 1024:.2f} MB")
            
            with open(output_path, "wb") as f:
                downloaded = 0
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        # Opcional: barra de progresso simples
                        # print(f"\r⬇️  Baixando: {downloaded/total_size*100:.1f}%", end="")
            
            print(f"\n✅ Sucesso! Arquivo salvo em: {output_path.absolute()}")
            return True
            
        elif response.status_code == 401:
            print("❌ Erro 401: Não autorizado. Verifique a API Key.")
            print(f"📨 Resposta: {response.text}")
            
        elif response.status_code == 404:
            print("⚠️  Erro 404: Arquivo não encontrado no bucket.")
            
        else:
            print(f"🔥 Erro {response.status_code}: Falha na requisição.")
            print(f"📨 Resposta: {response.text}")
            
        return False

    except Exception as e:
        print(f"❌ Exceção: {str(e)}")
        return False

if __name__ == "__main__":
    # Permite passar nome do arquivo por argumento
    target_file = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_FILE
    baixar_arquivo(target_file)

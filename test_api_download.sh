#!/bin/bash

# ==============================================================================
# Script de Teste de Conectividade - API Acervo SPU
# ==============================================================================
# Uso: ./test_api_download.sh [NOME_DO_ARQUIVO_OPCIONAL]
# Exemplo: ./test_api_download.sh documento_teste.pdf
# ==============================================================================

# --- Configurações ---
API_BASE_URL="https://spuservices.spu.gestao.gov.br"
API_KEY="98a7b6c5-d4e3-f2a1-b098-7c6d5e4f3a2b"
BUCKET="arquivos-acervo"

# Nome do arquivo para teste (pode ser passado via argumento ou usa padrão do notebook)
# Arquivo válido extraído de: documentacao/download-arquivos-acervo-cgdai.ipynb
FILE_NAME="${1:-1550846346565-null-8.tif-paraPDF.pdf}"
OUTPUT_FILE="downloaded_${FILE_NAME}"

echo "========================================================"
echo "🚀 Iniciando teste de API de Download do Acervo"
echo "========================================================"
echo "📍 URL Base: $API_BASE_URL"
echo "📦 Bucket:   $BUCKET"
echo "📄 Arquivo:  $FILE_NAME"
echo "--------------------------------------------------------"

# Executa o curl
# -w "%{http_code}" captura apenas o código HTTP no final
# -o salva o arquivo
# -s silencia o output de progresso padrão (nós criamos o nosso)
HTTP_CODE=$(curl -s -w "%{http_code}" -o "$OUTPUT_FILE" \
    -H "X-API-Key: $API_KEY" \
    "${API_BASE_URL}/acervo/arquivo/${BUCKET}/${FILE_NAME}")

echo "📡 Resposta HTTP: $HTTP_CODE"

# Análise do Resultado
if [ "$HTTP_CODE" == "200" ]; then
    echo "✅ SUCESSO: Download autorizado e realizado."
    echo "💾 Arquivo salvo em: $OUTPUT_FILE"
    
    # Verifica tamanho do arquivo
    FILE_SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)
    echo "📊 Tamanho: $FILE_SIZE"

elif [ "$HTTP_CODE" == "401" ]; then
    echo "❌ ERRO 401: Não autorizado."
    echo "   Verifique se a API Key está correta e válida."
    rm -f "$OUTPUT_FILE" # Remove arquivo vazio/de erro

elif [ "$HTTP_CODE" == "404" ]; then
    echo "⚠️  ALERTA 404: Arquivo não encontrado."
    echo "   A conexão com a API funcionou, mas o arquivo '$FILE_NAME' não existe no bucket."
    rm -f "$OUTPUT_FILE"

elif [ "$HTTP_CODE" == "500" ]; then
    echo "🔥 ERRO 500: Erro Interno do Servidor."
    echo "   Tente novamente mais tarde ou contate o suporte."
    rm -f "$OUTPUT_FILE"

else
    echo "❓ Status desconhecido: $HTTP_CODE"
    rm -f "$OUTPUT_FILE"
fi

echo "========================================================"

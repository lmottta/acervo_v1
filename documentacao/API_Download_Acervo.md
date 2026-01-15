# Documentação Técnica: API de Download do Acervo SPU

## 1. Visão Geral
Este documento detalha as especificações técnicas para integração com o serviço de recuperação de arquivos digitais do Acervo da Secretaria do Patrimônio da União (SPU).

**Base URL (Produção):**
`https://spuservices.spu.gestao.gov.br`

---

## 2. Autenticação e Segurança
O acesso aos endpoints é restrito e requer autenticação via **API Key**. A chave deve ser transmitida no cabeçalho (header) de cada requisição HTTP.

| Header | Valor (Exemplo) | Descrição |
| :--- | :--- | :--- |
| `X-API-Key` | `98a7b6c5-d4e3-f2a1-b098-7c6d5e4f3a2b` | Chave única de identificação do cliente consumidor. |

> **⚠️ Importante:** A API Key é uma credencial sensível. Não a exponha em repositórios públicos ou logs de cliente.

---

## 3. Endpoints

### 3.1. Download de Arquivo
Recupera o binário de um arquivo específico armazenado no Object Storage do Acervo.

**Método:** `GET`
**Rota:** `/acervo/arquivo/{bucket}/{nome_do_arquivo}`

#### Parâmetros de Rota (Path Parameters)

| Parâmetro | Tipo | Obrigatório | Descrição | Exemplo |
| :--- | :--- | :--- | :--- | :--- |
| `bucket` | String | Sim | Nome do bucket de origem no Object Storage. | `arquivos-acervo` |
| `nome_do_arquivo` | String | Sim | Identificador único ou caminho completo do arquivo. | `1550846346565-null-8.tif-paraPDF.pdf` |

#### Respostas (HTTP Status Codes)

| Código | Status | Descrição |
| :--- | :--- | :--- |
| **200** | `OK` | Requisição bem-sucedida. O corpo da resposta contém o binário do arquivo. |
| **401** | `Unauthorized` | Falha na autenticação. API Key inválida, expirada ou ausente no header. |
| **404** | `Not Found` | O arquivo solicitado não foi encontrado no bucket especificado. |
| **500** | `Internal Server Error` | Erro interno no servidor ou indisponibilidade momentânea do serviço. |

---

## 4. Coleção Postman

Para facilitar os testes e integração, disponibilizamos uma coleção do Postman contendo exemplos de requisições:

- **Arquivo:** [`Acervo_SPU_API.postman_collection.json`](./Acervo_SPU_API.postman_collection.json)
- **Funcionalidades Incluídas:**
    1.  **Download por Nome (GET):** Utiliza o endpoint `spuservices` com autenticação via Header.
    2.  **Download por ID (GET):** Utiliza o endpoint público `spunet` para acesso via identificador numérico.
    3.  **Pesquisa (POST):** Template para envio de critérios de busca via JSON.

### Como Importar
1.  Abra o Postman.
2.  Clique em **Import** (canto superior esquerdo).
3.  Arraste o arquivo `.json` ou selecione-o na pasta `documentacao`.
4.  As variáveis de ambiente (URLs, Chaves) já vêm pré-configuradas na coleção.

---

### 5. Exemplos de Uso com cURL

#### 5.1. Download por Nome (Autenticado - Baseado no Notebook)
Este é o método oficial identificado nos scripts de produção.

```bash
curl -X GET "https://spuservices.spu.gestao.gov.br/acervo/arquivo/arquivos-acervo/1550846346565-null-8.tif-paraPDF.pdf" \
  -H "X-API-Key: 98a7b6c5-d4e3-f2a1-b098-7c6d5e4f3a2b" \
  --output "download_auth.pdf"
```

#### 5.2. Download por ID (Endpoint Público Legado)
Utiliza o endpoint público que dispensa API Key (conforme cenário de uso).

```bash
curl -X GET "https://spunet.economia.gov.br/acervo/api/public/portal/arquivo-anexo/1550846346565" \
     --output "download_id.pdf"
```

### 5.3. Pesquisa de Arquivos (POST)
Exemplo de envio de critérios de filtro (payload JSON).

```bash
curl -X POST "https://spunet.economia.gov.br/acervo/api/public/portal/pesquisa" \
     -H "Content-Type: application/json" \
     -H "X-API-Key: 98a7b6c5-d4e3-f2a1-b098-7c6d5e4f3a2b" \
     -d '{
        "termo": "planta",
        "data_inicio": "2023-01-01",
        "data_fim": "2023-12-31",
        "tipo_documento": "cartografia"
     }'
```

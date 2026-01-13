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

## 4. Exemplo de Integração (cURL)

```bash
curl -X GET "https://spuservices.spu.gestao.gov.br/acervo/arquivo/arquivos-acervo/1550846346565-null-8.tif-paraPDF.pdf" \
     -H "X-API-Key: 98a7b6c5-d4e3-f2a1-b098-7c6d5e4f3a2b" \
     --output "downloaded_file.pdf"
```

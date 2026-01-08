# ETL - Extração tb_arquivo_acervo (Impala) → CSV → Email

Script ETL que extrai dados da tabela `tb_arquivo_acervo` do banco de dados Impala via haproxy, gera arquivo CSV e envia por email via mail-relay.

## 📋 Requisitos

- Python 3.10+
- Acesso à rede do haproxy do Impala
- Acesso ao mail-relay para envio de emails

## 🚀 Instalação

1. Instale as dependências:

```bash
pip install -r requirements.txt
```

## ⚙️ Configuração

As configurações estão no arquivo `main.py`:

### Conexão Impala
- `IMPALA_HOST`: Host do haproxy
- `IMPALA_PORT`: Porta (21050)
- `IMPALA_USER`: Usuário
- `IMPALA_PASSWORD`: Senha
- `IMPALA_DATABASE`: Banco de dados (`dbpro_11731_spunet_acervo`)
- `IMPALA_TABLE`: Tabela (`tb_arquivo_acervo`)

### Email
- `MAIL_SERVER`: Servidor SMTP (mail-relay)
- `MAIL_PORT`: Porta SMTP (25)
- `MAIL_TO`: Lista de destinatários
- `MAIL_FROM_ENVELOPE`: Remetente real (envelope)
- `MAIL_FROM_HEADER`: Remetente visível no email

### Limitação de Linhas
- `MAX_ROWS`: Número máximo de linhas a extrair (None = sem limite)

## 🎯 Uso

Execute o script:

```bash
python main.py
```

O script irá:
1. Conectar ao Impala via haproxy
2. Extrair todos os dados da tabela `tb_arquivo_acervo`
3. Gerar arquivo CSV em `data/output/tb_arquivo_acervo_YYYYMMDD_HHMMSS.csv`
4. Enviar email com o CSV anexado

## 📁 Estrutura de Saída

```
data/
  output/
    tb_arquivo_acervo_20250123_143025.csv
```

## 📧 Formato do Email

O email contém:
- Assunto: `Relatório tb_arquivo_acervo - DD/MM/YYYY HH:MM`
- Corpo: Estatísticas do relatório (linhas, colunas, tamanho)
- Anexo: Arquivo CSV com timestamp

## 🔧 Troubleshooting

### Erro de conexão Impala
- Verifique acesso à rede do haproxy
- Confirme credenciais e autenticação LDAP

### Erro de envio de email
- Verifique acesso ao mail-relay
- Confirme lista de destinatários válidos
- Verifique tamanho do arquivo (limites do SMTP)

### Arquivo muito grande
- Configure `MAX_ROWS` para limitar extração
- Considere compressão do CSV antes do envio

#!/usr/bin/env python3
"""
ETL Completo - Extração tb_arquivo_acervo (Impala) → Estruturação → Nextcloud → Email
Conecta via haproxy, gera CSV estruturado, publica no Nextcloud e envia email com link.
Compatível com Airflow DAG e execução standalone.
"""

import os
import sys
import re
import smtplib
import json
import socket
from datetime import datetime, timedelta, date
from email.message import EmailMessage
from email.utils import make_msgid, formatdate
from pathlib import Path
from typing import Optional, Dict, Any
from urllib.parse import quote

import pandas as pd
import numpy as np
from impala.dbapi import connect
from thrift.transport.TTransport import TTransportException
import requests
from requests.auth import HTTPBasicAuth

# Tentar importar Airflow (opcional, para execução standalone)
try:
    from airflow import DAG
    from airflow.hooks.base import BaseHook
    from airflow.operators.python import PythonOperator
    from airflow.models import Variable
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

# ----------------------------
# Configurações de Conexão Impala
# ----------------------------
IMPALA_HOST = os.getenv("IMPALA_HOST", "acessostageha.serpro.gov.br")
IMPALA_PORT = int(os.getenv("IMPALA_PORT", 21050))
IMPALA_USER = os.getenv("IMPALA_USER", "s_acstg_spu")
IMPALA_PASSWORD = os.getenv("IMPALA_PASSWORD", "6_Jp5?p08YnT.03E;35+4")
IMPALA_DATABASE = os.getenv("IMPALA_DATABASE", "dbpro_11731_spunet_acervo")
IMPALA_TABLE = os.getenv("IMPALA_TABLE", "tb_arquivo_acervo")
IMPALA_AUTH_MECHANISM = os.getenv("IMPALA_AUTH_MECHANISM", "LDAP")
IMPALA_USE_SSL = os.getenv("IMPALA_USE_SSL", "True").lower() in ('true', '1', 't')
IMPALA_TIMEOUT = int(os.getenv("IMPALA_TIMEOUT", 15))


# Airflow Connection ID (se disponível)
CONN_ID = "impala_via_haproxy"

# ----------------------------
# Configurações de Arquivo
# ----------------------------
BASE_DIR = Path(__file__).parent.absolute()
OUTPUT_DIR = BASE_DIR / "data/output"
PROCESSED_DIR = BASE_DIR / "data/processed"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# Nome fixo para Nextcloud (será sobrescrito mantendo mesmo share)
OUTPUT_FILENAME = "tb_arquivo_acervo_estruturado.xlsx"

# Limite de linhas (opcional)
MAX_ROWS = None  # None = sem limite

# ----------------------------
# SQL Principal
# ----------------------------
QUERY_ACERVO = """
-- =====================================================================
-- Script de Migração CSV - SPUNet Acervo para MemóriaSPU
-- VERSÃO PARA IMPALA (usando GROUP_CONCAT)
-- =====================================================================
SELECT 
    -- ========== CAMPOS DA TABELA PRINCIPAL tb_arquivo_acervo ==========
    
    -- Campo 1: ic_principal (FLAG) - converter para Sim/Não
    CASE 
        WHEN CAST(ta.ic_principal AS STRING) IN ('1', 'true', 'True') THEN 'Sim'
        WHEN CAST(ta.ic_principal AS STRING) IN ('0', 'false', 'False') THEN 'Não'
        ELSE ''
    END AS ic_principal,
    
    -- Campo 2: id_arquivo_acervo - converter para STRING
    CAST(ta.id_arquivo_acervo AS STRING) AS id_arquivo_acervo,
    
    -- Campo 3: co_protocolo_sei - CORRIGIDO: agregar pois vem de tb_arquivo_anexo (1:N)
    COALESCE(GROUP_CONCAT(DISTINCT tx.co_protocolo_sei, '||'), '') AS co_protocolo_sei,
    
    -- Campo 4: co_protocolo_acervo
    COALESCE(ta.co_protocolo_acervo, '') AS co_protocolo_acervo,
    
    -- Campo 5: id_arquivo_acervo_pai - converter para STRING
    COALESCE(CAST(ta.id_arquivo_acervo_pai AS STRING), '') AS id_arquivo_acervo_pai,
    
    -- Campo 6: dt_emissao_etiqueta - converter para STRING formato data
    COALESCE(CAST(ta.dt_emissao_etiqueta AS STRING), '') AS dt_emissao_etiqueta,
    
    -- Campo 7: id_arquivo_acervo_1 (Arquivos associados) - IMPALA: GROUP_CONCAT
    COALESCE(GROUP_CONCAT(DISTINCT CAST(tq.id_arquivo_acervo_1 AS STRING), '||'), '') AS id_arquivo_acervo_1,
    
    -- Campo 8: id_arquivo_acervo_2 (Arquivos associados) - IMPALA: GROUP_CONCAT
    COALESCE(GROUP_CONCAT(DISTINCT CAST(tq1.id_arquivo_acervo_2 AS STRING), '||'), '') AS id_arquivo_acervo_2,
    
    -- Campo 9: ds_categoria (Gênero documental)
    COALESCE(tc.ds_categoria, '') AS ds_categoria,
    
    -- ========== CAMPOS DE tb_descritoes_informacionais_iconografico ==========
    
    -- Campo 10: nu_area_equivalente - converter para STRING
    COALESCE(CAST(td.nu_area_equivalente AS STRING), '') AS nu_area_equivalente,
    
    -- Campo 11: nu_area_total - converter para STRING
    COALESCE(CAST(td.nu_area_total AS STRING), '') AS nu_area_total,
    
    -- Campo 12: ds_bairro - mesclagem entre td e tdi
    COALESCE(
        CASE WHEN td.ds_bairro IS NOT NULL AND td.ds_bairro != '' THEN td.ds_bairro ELSE NULL END,
        CASE WHEN tdi.ds_bairro IS NOT NULL AND tdi.ds_bairro != '' THEN tdi.ds_bairro ELSE NULL END,
        ''
    ) AS ds_bairro,
    
    -- Campo 13: ds_confrontacoes - mesclagem entre td e tdi
    COALESCE(
        CASE WHEN td.ds_confrontacoes IS NOT NULL AND td.ds_confrontacoes != '' THEN td.ds_confrontacoes ELSE NULL END,
        CASE WHEN tdi.ds_confrontacoes IS NOT NULL AND tdi.ds_confrontacoes != '' THEN tdi.ds_confrontacoes ELSE NULL END,
        ''
    ) AS ds_confrontacoes,
    
    -- Campo 14: nu_escala_cartografica - converter para STRING
    COALESCE(CAST(td.nu_escala_cartografica AS STRING), '') AS nu_escala_cartografica,
    
    -- Campo 15: ds_informacoes_complementares - mesclagem
    COALESCE(
        CASE WHEN td.ds_informacoes_complementares IS NOT NULL AND td.ds_informacoes_complementares != '' 
             THEN td.ds_informacoes_complementares ELSE NULL END,
        CASE WHEN tdi.ds_info_complementares IS NOT NULL AND tdi.ds_info_complementares != '' 
             THEN tdi.ds_info_complementares ELSE NULL END,
        ''
    ) AS ds_informacoes_complementares_iconografico,
    
    -- Campo 16: ds_logradouro_atualizado - mesclagem entre td e tdi
    COALESCE(
        CASE WHEN td.ds_logradouro_atualizado IS NOT NULL AND td.ds_logradouro_atualizado != '' 
             THEN td.ds_logradouro_atualizado ELSE NULL END,
        CASE WHEN tdi.ds_logradouro_atualizado IS NOT NULL AND tdi.ds_logradouro_atualizado != '' 
             THEN tdi.ds_logradouro_atualizado ELSE NULL END,
        ''
    ) AS ds_logradouro_atualizado,
    
    -- Campo 17: ds_logradouro_original - mesclagem entre td e tdi
    COALESCE(
        CASE WHEN td.ds_logradouro_original IS NOT NULL AND td.ds_logradouro_original != '' 
             THEN td.ds_logradouro_original ELSE NULL END,
        CASE WHEN tdi.ds_logradouro_original IS NOT NULL AND tdi.ds_logradouro_original != '' 
             THEN tdi.ds_logradouro_original ELSE NULL END,
        ''
    ) AS ds_logradouro_original,
    
    -- Campo 18: nu_metro - converter para STRING
    COALESCE(CAST(tum.nu_metro AS STRING), '') AS nu_metro,
    
    -- Campo 19: nu_rip - mesclagem entre td e tdi, converter para STRING
    COALESCE(
        CASE WHEN td.nu_rip IS NOT NULL THEN CAST(td.nu_rip AS STRING) ELSE NULL END,
        CASE WHEN tdi.nu_rip IS NOT NULL THEN CAST(tdi.nu_rip AS STRING) ELSE NULL END,
        ''
    ) AS nu_rip,
    
    -- Campo 20: ic_vinculado_spu (FLAG) - converter para Sim/Não
    CASE 
        WHEN CAST(td.ic_vinculado_spu AS STRING) IN ('1', 'true', 'True') THEN 'Sim'
        WHEN CAST(td.ic_vinculado_spu AS STRING) IN ('0', 'false', 'False') THEN 'Não'
        ELSE ''
    END AS ic_vinculado_spu,
    
    -- Campo 21: ds_orgao_emissor - mesclagem entre td e tdi
    COALESCE(
        CASE WHEN td.ds_orgao_emissor IS NOT NULL AND td.ds_orgao_emissor != '' 
             THEN td.ds_orgao_emissor ELSE NULL END,
        CASE WHEN tdi.ds_orgao_emissor IS NOT NULL AND tdi.ds_orgao_emissor != '' 
             THEN tdi.ds_orgao_emissor ELSE NULL END,
        ''
    ) AS ds_orgao_emissor,
    
    -- Campo 22: ds_regiao_administrativa
    COALESCE(td.ds_regiao_administrativa, '') AS ds_regiao_administrativa,
    
    -- Campo 23: ds_tipo_documento
    COALESCE(td.ds_tipo_documento, '') AS ds_tipo_documento,
    
    -- Campo 24: ds_autor_copia - IMPALA: GROUP_CONCAT
    COALESCE(GROUP_CONCAT(DISTINCT tac.ds_autor_copia, '||'), '') AS ds_autor_copia,
    
    -- Campo 25: ds_autor_original - IMPALA: GROUP_CONCAT
    COALESCE(GROUP_CONCAT(DISTINCT tao.ds_autor_original, '||'), '') AS ds_autor_original,
    
    -- Campo 26: ds_processo_vinculado - IMPALA: GROUP_CONCAT
    COALESCE(GROUP_CONCAT(DISTINCT tpv.ds_processo_vinculado, '||'), '') AS ds_processo_vinculado,
    
    -- Campo 27: ds_palavra_chave_iconografico - IMPALA: GROUP_CONCAT
    COALESCE(GROUP_CONCAT(DISTINCT tpc.ds_palavra_chave, '||'), '') AS ds_palavra_chave_iconografico,
    
    -- Campo 28: ds_municipio
    COALESCE(tm.ds_municipio, '') AS ds_municipio,
    
    -- Campo 29: ds_tipo_documental
    COALESCE(ttd.ds_tipo_documental, '') AS ds_tipo_documental,
    
    -- Campo 30: ds_unidade_federativa
    COALESCE(tuf.ds_unidade_federativa, '') AS ds_unidade_federativa,
    
    -- Campo 31: ds_unidade_medida
    COALESCE(tum.ds_unidade_medida, '') AS ds_unidade_medida,
    
    -- ========== CAMPOS DE tb_descritor_catalografico ==========
    
    -- Campo 32: ds_caixa
    COALESCE(tdc.ds_caixa, '') AS ds_caixa,
    
    -- Campo 33: ds_estante
    COALESCE(tdc.ds_estante, '') AS ds_estante,
    
    -- Campo 34: ic_iconografia (FLAG) - converter para Sim/Não
    CASE 
        WHEN CAST(tdc.ic_iconografia AS STRING) IN ('1', 'true', 'True') THEN 'Sim'
        WHEN CAST(tdc.ic_iconografia AS STRING) IN ('0', 'false', 'False') THEN 'Não'
        ELSE ''
    END AS ic_iconografia,
    
    -- Campo 35: tx_informacoes_complementares - mesclagem
    COALESCE(
        CASE WHEN tdc.tx_informacoes_complementares IS NOT NULL AND tdc.tx_informacoes_complementares != '' 
             THEN tdc.tx_informacoes_complementares ELSE NULL END,
        CASE WHEN tdci.ds_info_complementares IS NOT NULL AND tdci.ds_info_complementares != '' 
             THEN tdci.ds_info_complementares ELSE NULL END,
        ''
    ) AS tx_informacoes_complementares,
    
    -- Campo 36: ic_notacao_kardex (FLAG) - converter para Sim/Não
    CASE 
        WHEN CAST(tdc.ic_notacao_kardex AS STRING) IN ('1', 'true', 'True') THEN 'Sim'
        WHEN CAST(tdc.ic_notacao_kardex AS STRING) IN ('0', 'false', 'False') THEN 'Não'
        ELSE ''
    END AS ic_notacao_kardex,
    
    -- Campo 37: ic_especie_cartografia (FLAG) - converter para Sim/Não
    CASE 
        WHEN tdc.ic_especie_cartografia = 1 OR tdc.ic_especie_cartografia = true THEN 'Sim'
        WHEN tdc.ic_especie_cartografia = 0 OR tdc.ic_especie_cartografia = false THEN 'Não'
        ELSE ''
    END AS ic_especie_cartografia,
    
    -- Campo 38: ic_especie_fotografia (FLAG) - converter para Sim/Não
    CASE 
        WHEN tdc.ic_especie_fotografia = 1 OR tdc.ic_especie_fotografia = true THEN 'Sim'
        WHEN tdc.ic_especie_fotografia = 0 OR tdc.ic_especie_fotografia = false THEN 'Não'
        ELSE ''
    END AS ic_especie_fotografia,
    
    -- Campo 39: ds_prateleira
    COALESCE(tdc.ds_prateleira, '') AS ds_prateleira,
    
    -- Campo 40: ds_condicao_original
    COALESCE(tco.ds_condicao_original, '') AS ds_condicao_original,
    
    -- Campo 41: ds_dimensao_suporte
    COALESCE(tds.ds_dimensao_suporte, '') AS ds_dimensao_suporte,
    
    -- ========== CAMPOS DE tb_descritor_catalografico_iconografico ==========
    
    -- Campo 42: ds_armario
    COALESCE(tdci.ds_armario, '') AS ds_armario,
    
    -- Campo 43: ds_gaveta
    COALESCE(tdci.ds_gaveta, '') AS ds_gaveta,
    
    -- Campo 44: ds_info_complementares - mesclagem (inversa da campo 35)
    COALESCE(
        CASE WHEN tdci.ds_info_complementares IS NOT NULL AND tdci.ds_info_complementares != '' 
             THEN tdci.ds_info_complementares ELSE NULL END,
        CASE WHEN tdc.tx_informacoes_complementares IS NOT NULL AND tdc.tx_informacoes_complementares != '' 
             THEN tdc.tx_informacoes_complementares ELSE NULL END,
        ''
    ) AS ds_info_complementares,
    
    -- Campo 45: ds_mapoteca
    COALESCE(tdci.ds_mapoteca, '') AS ds_mapoteca,
    
    -- Campo 46: ds_orgao_tutela
    COALESCE(tdci.ds_orgao_tutela, '') AS ds_orgao_tutela,
    
    -- Campo 47: ds_orgao_origem
    COALESCE(tdci.ds_orgao_origem, '') AS ds_orgao_origem,
    
    -- Campo 48: ds_planta
    COALESCE(tdci.ds_planta, '') AS ds_planta,
    
    -- Campo 49: ds_prancha
    COALESCE(tdci.ds_prancha, '') AS ds_prancha,
    
    -- Campo 50: ds_item
    COALESCE(tdci.ds_item, '') AS ds_item,
    
    -- Campo 51: ds_tela
    COALESCE(tdci.ds_tela, '') AS ds_tela,
    
    -- Campo 52: ds_tubo
    COALESCE(tdci.ds_tubo, '') AS ds_tubo,
    
    -- Campo 55: ds_documento
    COALESCE(tdoc.ds_documento, '') AS ds_documento,
    
    -- Campo 56: ds_material_suporte
    COALESCE(tms.ds_material_suporte, '') AS ds_material_suporte,
    
    -- ========== CAMPOS DE tb_descritoes_informacionais ==========
    
    -- Campo 58: ds_cep
    COALESCE(tdi.ds_cep, '') AS ds_cep,
    
    -- Campo 60: dt_atuacao_processo - converter para STRING
    COALESCE(CAST(tdi.dt_atuacao_processo AS STRING), '') AS dt_atuacao_processo,
    
    -- Campo 61: ds_denominacao_patrimonial
    COALESCE(tdi.ds_denominacao_patrimonial, '') AS ds_denominacao_patrimonial,
    
    -- Campo 65: ds_processo_administrativo
    COALESCE(tdi.ds_processo_administrativo, '') AS ds_processo_administrativo,
    
    -- Campo 66: ds_processo_outro_orgao
    COALESCE(tdi.ds_processo_outro_orgao, '') AS ds_processo_outro_orgao,
    
    -- Campo 68: nu_sei
    COALESCE(tdi.nu_sei, '') AS nu_sei,
    
    -- Campo 69: nu_rip_spiu
    COALESCE(tdi.nu_rip_spiu, '') AS nu_rip_spiu,
    
    -- Campo 70: nu_rip_siapa
    COALESCE(tdi.nu_rip_siapa, '') AS nu_rip_siapa,
    
    -- Campo 72: ds_pasta_requisito_ref
    COALESCE(tdi.ds_pasta_requisito_ref, '') AS ds_pasta_requisito_ref,
    
    -- Campo 73: ds_titulo_formal
    COALESCE(tdi.ds_titulo_formal, '') AS ds_titulo_formal,
    
    -- Campo 74: ds_interessado - IMPALA: GROUP_CONCAT
    COALESCE(GROUP_CONCAT(DISTINCT ti.ds_interessado, '||'), '') AS ds_interessado,
    
    -- Campo 76: ds_processo_antecedente - IMPALA: GROUP_CONCAT
    COALESCE(GROUP_CONCAT(DISTINCT tpa.ds_processo_antecedente, '||'), '') AS ds_processo_antecedente,
    
    -- Campo 77: ds_assunto
    COALESCE(tas.ds_assunto, '') AS ds_assunto,
    
    -- Campo 78: ds_especie_documental
    COALESCE(ted.ds_especie_documental, '') AS ds_especie_documental,
    
    -- Campo 80: ds_patrimonio
    COALESCE(tp.ds_patrimonio, '') AS ds_patrimonio,
    
    -- ========== CAMPOS ESPECIAIS ==========
    
    -- special_document - campo especial (vazio conforme documentação)
    '' AS special_document,
    
    -- special_attachments - URLs dos anexos agregados - IMPALA: GROUP_CONCAT
    COALESCE(
        GROUP_CONCAT(DISTINCT CONCAT('https://spunet.economia.gov.br/acervo/api/public/portal/arquivo-anexo/', CAST(tx.id_arquivo_anexo AS STRING),''), '||'),
        ''
    ) AS special_attachments,

    -- ========== CAMPOS PARA PROCESSAMENTO INTERNO (PYTHON) ==========
    ta.id_categoria,
    ta.id_situacao,
    ta.id_descritor_informacional,
    ta.id_descritor_catalografico,
    ta.id_descritor_tecnico,
    ta.id_descritoes_informacionais_iconografico,
    ta.id_descritor_catalografico_iconografico

FROM tb_arquivo_acervo ta

-- ========== JOINS ==========

LEFT JOIN tb_arquivo_anexo tx ON tx.id_arquivo_acervo = ta.id_arquivo_acervo
LEFT JOIN tb_arquivo_acervo_associado tq ON tq.id_arquivo_acervo_1 = ta.id_arquivo_acervo
LEFT JOIN tb_arquivo_acervo_associado tq1 ON tq1.id_arquivo_acervo_2 = ta.id_arquivo_acervo
LEFT JOIN tb_categoria tc ON tc.id_categoria = ta.id_categoria
LEFT JOIN tb_descritoes_informacionais_iconografico td ON td.id_descritoes_informacionais_iconografico = ta.id_descritoes_informacionais_iconografico
LEFT JOIN tb_autor_copia tac ON tac.id_descritoes_informacionais_iconografico = td.id_descritoes_informacionais_iconografico
LEFT JOIN tb_autor_original tao ON tao.id_descritoes_informacionais_iconografico = td.id_descritoes_informacionais_iconografico
LEFT JOIN tb_processo_vinculado tpv ON tpv.id_descritoes_informacionais_iconografico = td.id_descritoes_informacionais_iconografico
LEFT JOIN tb_palavra_chave_descritoes_informacionais_iconografico tpcd ON tpcd.id_descritoes_informacionais_iconografico = ta.id_descritoes_informacionais_iconografico
LEFT JOIN tb_palavra_chave tpc ON tpc.id_palavra_chave = tpcd.id_palavra_chave
LEFT JOIN tb_municipio tm ON tm.id_municipio = td.id_municipio
LEFT JOIN tb_tipo_documental ttd ON ttd.id_tipo_documental = td.id_tipo_documental
LEFT JOIN tb_unidade_federativa tuf ON tuf.id_unidade_federativa = td.id_unidade_federativa
LEFT JOIN tb_unidade_medida tum ON tum.id_unidade_medida = td.id_unidade_medida
LEFT JOIN tb_descritor_catalografico tdc ON tdc.id_descritor_catalografico = ta.id_descritor_catalografico
LEFT JOIN tb_condicao_original tco ON tco.id_condicao_original = tdc.id_condicao_original
LEFT JOIN tb_dimensao_suporte tds ON tds.id_dimensao_suporte = tdc.id_dimensao_suporte
LEFT JOIN tb_descritor_catalografico_iconografico tdci ON tdci.id_descritor_catalografico_iconografico = ta.id_descritor_catalografico_iconografico
LEFT JOIN tb_documento tdoc ON tdoc.id_documento = tdci.id_documento
LEFT JOIN tb_material_suporte tms ON tms.id_material_suporte = tdci.id_material_suporte
LEFT JOIN tb_descritoes_informacionais tdi ON tdi.id_descritor_informacional = ta.id_descritor_informacional
LEFT JOIN tb_interessado ti ON ti.id_descritor_informacional = tdi.id_descritor_informacional
LEFT JOIN tb_processo_antecedente tpa ON tpa.id_descritor_informacional = tdi.id_descritor_informacional
LEFT JOIN tb_assunto tas ON tas.id_assunto = tdi.co_assunto
LEFT JOIN tb_especie_documental ted ON ted.id_especie_documental = tdi.id_especie_documental
LEFT JOIN tb_patrimonio tp ON tp.id_patrimonio = tdi.id_patrimonio

-- ========== FILTROS ==========

WHERE 
    (CAST(ta.ic_publicado_internet AS STRING) IN ('1', 'true', 'True'))
    AND (ta.id_situacao IS NULL OR ta.id_situacao != 2)

-- ========== GROUP BY (NECESSÁRIO para GROUP_CONCAT) ==========

GROUP BY
    ta.ic_principal,
    ta.id_arquivo_acervo,
    ta.co_protocolo_acervo,
    ta.id_arquivo_acervo_pai,
    ta.dt_emissao_etiqueta,
    tc.ds_categoria,
    td.nu_area_equivalente,
    td.nu_area_total,
    td.ds_bairro,
    td.ds_confrontacoes,
    td.nu_escala_cartografica,
    td.ds_informacoes_complementares,
    td.ds_logradouro_atualizado,
    td.ds_logradouro_original,
    tum.nu_metro,
    td.nu_rip,
    td.ic_vinculado_spu,
    td.ds_orgao_emissor,
    td.ds_regiao_administrativa,
    td.ds_tipo_documento,
    tm.ds_municipio,
    ttd.ds_tipo_documental,
    tuf.ds_unidade_federativa,
    tum.ds_unidade_medida,
    tdc.ds_caixa,
    tdc.ds_estante,
    tdc.ic_iconografia,
    tdc.tx_informacoes_complementares,
    tdc.ic_notacao_kardex,
    tdc.ic_especie_cartografia,
    tdc.ic_especie_fotografia,
    tdc.ds_prateleira,
    tco.ds_condicao_original,
    tds.ds_dimensao_suporte,
    tdci.ds_armario,
    tdci.ds_gaveta,
    tdci.ds_info_complementares,
    tdci.ds_mapoteca,
    tdci.ds_orgao_tutela,
    tdci.ds_orgao_origem,
    tdci.ds_planta,
    tdci.ds_prancha,
    tdci.ds_item,
    tdci.ds_tela,
    tdci.ds_tubo,
    tdoc.ds_documento,
    tms.ds_material_suporte,
    tdi.ds_bairro,
    tdi.ds_cep,
    tdi.ds_confrontacoes,
    tdi.dt_atuacao_processo,
    tdi.ds_denominacao_patrimonial,
    tdi.ds_info_complementares,
    tdi.ds_logradouro_atualizado,
    tdi.ds_logradouro_original,
    tdi.ds_processo_administrativo,
    tdi.ds_processo_outro_orgao,
    tdi.nu_rip,
    tdi.nu_sei,
    tdi.nu_rip_spiu,
    tdi.nu_rip_siapa,
    tdi.ds_orgao_emissor,
    tdi.ds_pasta_requisito_ref,
    tdi.ds_titulo_formal,
    tas.ds_assunto,
    ted.ds_especie_documental,
    tp.ds_patrimonio,
    -- Campos Adicionados para Processamento Interno
    ta.id_categoria,
    ta.id_situacao,
    ta.id_descritor_informacional,
    ta.id_descritor_catalografico,
    ta.id_descritor_tecnico,
    ta.id_descritoes_informacionais_iconografico,
    ta.id_descritor_catalografico_iconografico

-- ========== ORDENAÇÃO ==========

ORDER BY ta.id_arquivo_acervo
"""

# ----------------------------
# Configurações de Email
# ----------------------------
MAIL_SERVER = os.getenv("MAIL_SERVER", "mail-relay")
MAIL_PORT = int(os.getenv("MAIL_PORT", "25"))
MAIL_FROM_ENVELOPE = os.getenv("MAIL_FROM_ENVELOPE", "dev.lamota@gmail.com")
MAIL_FROM_HEADER = os.getenv("MAIL_FROM_HEADER", "SPU – Relatórios <noreply@spu.gov.br>")
MAIL_REPLY_TO = os.getenv("MAIL_REPLY_TO", "noreply@spu.gov.br")
MAIL_EHLO_HOST = os.getenv("MAIL_EHLO_HOST", "spu.gov.br")

# Lê destinatários das variáveis de ambiente, separados por vírgula
_mail_to_str = os.getenv("MAIL_TO", "leonardo.mota@gestao.gov.br,flavio-s.araujo@gestao.gov.br")
MAIL_TO = [email.strip() for email in _mail_to_str.split(",") if email.strip()]


# ----------------------------
# Configurações Nextcloud
# ----------------------------
# Se executando no Airflow, busca das Variables. Caso contrário, use valores padrão.
if AIRFLOW_AVAILABLE:
    try:
        NC_BASE = Variable.get("NEXTCLOUD_BASE_URL")
        NC_USER = Variable.get("NEXTCLOUD_USERNAME")
        NC_PASS = Variable.get("NEXTCLOUD_APP_PASSWORD")
        # Tenta NEXTCLOUD_ACERVO_DIR, depois NEXTCLOUD_GEOJSON_DIR como fallback
        try:
            NC_DIR = Variable.get("NEXTCLOUD_ACERVO_DIR")
        except:
            try:
                NC_DIR = Variable.get("NEXTCLOUD_GEOJSON_DIR")
            except:
                NC_DIR = "CGDAI/Relatorios/acervo"
        VERIFY_SSL = Variable.get("NEXTCLOUD_VERIFY_SSL", default_var="true").lower() == "true"
        NC_SHARE_PASSWORD = Variable.get("NEXTCLOUD_SHARE_PASSWORD", default_var=None)
        NC_SHARE_EXPIRE_DAYS = int(Variable.get("NEXTCLOUD_SHARE_EXPIRE_DAYS", default_var="0") or "0")
    except:
        # Fallback para execução standalone
        NC_BASE = None
        NC_USER = None
        NC_PASS = None
        NC_DIR = None
        VERIFY_SSL = True
        NC_SHARE_PASSWORD = None
        NC_SHARE_EXPIRE_DAYS = 0
else:
    # Configurações para execução standalone (ajuste conforme necessário)
    NC_BASE = os.getenv("NEXTCLOUD_BASE_URL")  # ex: https://spudrive.duckdns.org
    NC_USER = os.getenv("NEXTCLOUD_USERNAME")
    NC_PASS = os.getenv("NEXTCLOUD_APP_PASSWORD")
    NC_DIR = os.getenv("NEXTCLOUD_DIR", "CGDAI/Relatorios/acervo")
    VERIFY_SSL = os.getenv("NEXTCLOUD_VERIFY_SSL", "true").lower() == "true"
    NC_SHARE_PASSWORD = os.getenv("NEXTCLOUD_SHARE_PASSWORD")
    NC_SHARE_EXPIRE_DAYS = int(os.getenv("NEXTCLOUD_SHARE_EXPIRE_DAYS", "0") or "0")

# ----------------------------
# Helpers
# ----------------------------
_illegal_re = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")


def sanitize_value(v):
    """Remove caracteres ilegais para CSV/Excel."""
    if v is None:
        return v
    if isinstance(v, str):
        return _illegal_re.sub("", v)
    return v


def sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Sanitiza todas as colunas do DataFrame."""
    df_copy = df.copy()
    for col in df_copy.columns:
        if df_copy[col].dtype == object:
            df_copy[col] = df_copy[col].map(sanitize_value)
    return df_copy


def _get_impala_params():
    """Obtém parâmetros de conexão Impala (Airflow ou direto)."""
    if AIRFLOW_AVAILABLE:
        try:
            conn = BaseHook.get_connection(CONN_ID)
            extras = conn.extra_dejson or {}
            return {
                "host": conn.host,
                "port": conn.port or 21050,
                "user": conn.login,
                "password": conn.password,
                "auth_mechanism": extras.get("auth_mechanism") or extras.get("extra__generic__auth_mechanism") or "LDAP",
                "use_ssl": extras.get("use_ssl") in (True, "true", "True", "1") or extras.get("extra__generic__use_ssl") in (True, "true", "True", "1"),
            }
        except:
            pass
    
    # Fallback para configuração direta
    return {
        "host": IMPALA_HOST,
        "port": IMPALA_PORT,
        "user": IMPALA_USER,
        "password": IMPALA_PASSWORD,
        "auth_mechanism": IMPALA_AUTH_MECHANISM,
        "use_ssl": IMPALA_USE_SSL,
    }


# ----------------------------
# Funções Nextcloud
# ----------------------------
def _webdav_url_for(path: str, base: str, user: str) -> str:
    """Monta URL WebDAV para caminho."""
    safe_path = "/".join(quote(p) for p in path.split("/")) if path else ""
    return f"{base}/remote.php/dav/files/{quote(user)}/{safe_path}"


def _ocs_url(path: str, base: str) -> str:
    """Monta URL OCS API."""
    return f"{base}/ocs/v2.php{path}"


def _make_session(user: str, password: str, verify: bool = True) -> requests.Session:
    """Cria sessão requests autenticada."""
    s = requests.Session()
    s.auth = HTTPBasicAuth(user, password)
    s.verify = verify
    return s


def _ensure_directory(session: requests.Session, dir_path: str, base: str, user: str):
    """Cria diretórios recursivamente no Nextcloud."""
    path = (dir_path or "").strip("/")
    if not path:
        return
    parts = path.split("/")
    current = ""
    for part in parts:
        current = f"{current}/{part}" if current else part
        url = _webdav_url_for(current, base, user)
        resp = session.request("MKCOL", url)
        if resp.status_code in (201, 405):
            continue
        check = session.request("PROPFIND", url, headers={"Depth": "0"})
        if check.status_code not in (200, 207):
            raise RuntimeError(f"Falha ao criar diretório {current}: {resp.status_code} {resp.text}")


def _upload_file(session: requests.Session, local_path: str, remote_rel_path: str, base: str, user: str):
    """Upload de arquivo via WebDAV PUT."""
    url = _webdav_url_for(remote_rel_path, base, user)
    with open(local_path, "rb") as f:
        resp = session.put(url, data=f)
    if resp.status_code not in (200, 201, 204):
        raise RuntimeError(f"Falha no upload {remote_rel_path}: {resp.status_code} {resp.text}")


def _get_existing_public_share(session: requests.Session, rel_path: str, base: str):
    """Retorna share público existente do arquivo."""
    headers = {"OCS-APIRequest": "true", "Accept": "application/json"}
    params = {"path": f"/{rel_path.strip('/')}", "format": "json"}
    url = _ocs_url("/apps/files_sharing/api/v1/shares", base)
    resp = session.get(url, headers=headers, params=params)
    if resp.status_code != 200:
        raise RuntimeError(f"OCS shares GET falhou: {resp.status_code} {resp.text}")
    data = resp.json()
    entries = data.get("ocs", {}).get("data", []) if data else []
    for e in entries:
        if str(e.get("share_type")) == "3" and e.get("item_type") == "file":
            if e.get("path") == f"/{rel_path.strip('/')}":
                return e
    return None


def _create_public_share(session: requests.Session, rel_path: str, base: str, password: Optional[str] = None, expire_days: int = 0):
    """Cria link público para arquivo."""
    headers = {"OCS-APIRequest": "true", "Accept": "application/json"}
    url = _ocs_url("/apps/files_sharing/api/v1/shares", base)
    payload = {
        "path": f"/{rel_path.strip('/')}",
        "shareType": 3,
        "permissions": 1,
    }
    if password:
        payload["password"] = password
    if expire_days and expire_days > 0:
        exp = date.today() + timedelta(days=expire_days)
        payload["expireDate"] = exp.isoformat()
    
    resp = session.post(url, headers=headers, data=payload)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"OCS share POST falhou: {resp.status_code} {resp.text}")
    data = resp.json()
    meta = data.get("ocs", {}).get("meta", {})
    if meta.get("status") != "ok":
        raise RuntimeError(f"OCS share POST erro: {json.dumps(data)}")
    return data["ocs"]["data"]


def _update_public_share(session: requests.Session, share_id: str, base: str, password: Optional[str] = None, expire_days: int = 0):
    """Atualiza senha e/ou expiração de share existente."""
    headers = {"OCS-APIRequest": "true", "Accept": "application/json"}
    url = _ocs_url(f"/apps/files_sharing/api/v1/shares/{share_id}", base)
    if password is not None:
        resp = session.put(url, headers=headers, data={"password": password})
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"OCS share PUT (password) falhou: {resp.status_code} {resp.text}")
    if expire_days and expire_days > 0:
        exp = date.today() + timedelta(days=expire_days)
        resp = session.put(url, headers=headers, data={"expireDate": exp.isoformat()})
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"OCS share PUT (expireDate) falhou: {resp.status_code} {resp.text}")


def publish_to_nextcloud(local_file: Path, filename: str) -> Dict[str, Optional[str]]:
    """
    Publica arquivo no Nextcloud e cria/atualiza share público.
    
    Returns:
        Dict com share_url, direct_download_url, remote_path
    """
    if not all([NC_BASE, NC_USER, NC_PASS]):
        print("⚠️  Nextcloud não configurado, pulando publicação...")
        return {"share_url": None, "direct_download_url": None, "remote_path": None}
    
    print(f"\n☁️  Publicando no Nextcloud: {filename}")
    
    remote_dir = (NC_DIR or "").strip("/")
    remote_rel_path = f"{remote_dir}/{filename}" if remote_dir else filename
    
    session = _make_session(NC_USER, NC_PASS, VERIFY_SSL)
    
    # 1) Garante diretório
    _ensure_directory(session, remote_dir, NC_BASE, NC_USER)
    
    # 2) Upload (PUT) sobrescrevendo
    _upload_file(session, str(local_file), remote_rel_path, NC_BASE, NC_USER)
    print(f"✅ Arquivo enviado para: /{remote_rel_path}")
    
    # 3) Share público
    existing = _get_existing_public_share(session, remote_rel_path, NC_BASE)
    if existing:
        share_id = str(existing.get("id"))
        _update_public_share(session, share_id, NC_BASE, NC_SHARE_PASSWORD, NC_SHARE_EXPIRE_DAYS)
        share_url = existing.get("url")
        print(f"✅ Share público atualizado: {share_url}")
    else:
        created = _create_public_share(session, remote_rel_path, NC_BASE, NC_SHARE_PASSWORD, NC_SHARE_EXPIRE_DAYS)
        share_id = str(created.get("id"))
        share_url = created.get("url")
        print(f"✅ Share público criado: {share_url}")
    
    direct_download_url = f"{share_url}/download" if share_url else None
    
    return {
        "share_url": share_url,
        "direct_download_url": direct_download_url,
        "remote_path": remote_rel_path,
    }


# ----------------------------
# Funções de ETL
# ----------------------------
def extract_data_from_impala() -> pd.DataFrame:
    """Conecta ao Impala e extrai dados."""
    print(f"🔌 Conectando ao Impala: {IMPALA_HOST}:{IMPALA_PORT}")
    print(f"📊 Banco: {IMPALA_DATABASE}")
    print(f"📋 Tabela: {IMPALA_TABLE}")
    
    query = QUERY_ACERVO
    if MAX_ROWS:
        query += f" LIMIT {MAX_ROWS}"
    
    conn_params = _get_impala_params()
    
    try:
        with connect(database=IMPALA_DATABASE, **conn_params) as impala_conn:
            print("✅ Conexão estabelecida!")
            print("⏳ Executando query...")
            df = pd.read_sql(query, impala_conn)
            print(f"✅ Dados extraídos: {len(df):,} linhas, {len(df.columns)} colunas")
            return df
    except Exception as e:
        print(f"❌ Erro ao conectar/extrair: {e}")
        raise


def structure_data(df: pd.DataFrame) -> pd.DataFrame:
    """Estrutura e transforma os dados."""
    print("\n🔧 Estruturando dados...")
    
    df = df.copy()
    
    # 1. Normalização de tipos
    
    # IDs internos (inteiros) usados para lógica de campos calculados
    int_id_cols = [
        "id_categoria",
        "id_situacao",
        "id_descritor_informacional",
        "id_descritor_catalografico",
        "id_descritor_tecnico",
        "id_descritoes_informacionais_iconografico",
        "id_descritor_catalografico_iconografico",
    ]
    for col in int_id_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # IDs e campos que devem ser Strings (já vêm como string do SQL, mas garantimos)
    str_cols = [
        "id_arquivo_acervo",
        "id_arquivo_acervo_pai",
        "id_arquivo_acervo_1", 
        "id_arquivo_acervo_2",
        "nu_rip",
        "nu_rip_spiu",
        "nu_rip_siapa",
        "special_attachments",
        "ic_principal",
        "ic_vinculado_spu",
        "ic_iconografia",
        "ic_notacao_kardex",
        "ic_especie_cartografia",
        "ic_especie_fotografia"
    ]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype("string")
    
    # Converter data
    if "dt_emissao_etiqueta" in df.columns:
        df["dt_emissao_etiqueta"] = pd.to_datetime(df["dt_emissao_etiqueta"], errors="coerce")

    # Tratamento para Protocolo Acervo (evitar notação científica no Excel)
    # Garante que seja tratado como texto, preservando o valor real
    if "co_protocolo_acervo" in df.columns:
        def _format_protocol(val):
            if pd.isna(val) or val == "":
                return None
            try:
                # Se for número (float/int), converte para int p/ remover decimal e então string
                # Adiciona apóstrofo (') para forçar Excel a tratar como TEXTO e não Científico
                val_str = str(int(float(val)))
                return f"'{val_str}"
            except:
                # Se não for número, mantém como string original
                return str(val)
        
        df["co_protocolo_acervo"] = df["co_protocolo_acervo"].apply(_format_protocol).astype("string")
    
    # 2. Campos derivados
    # Para funcionar, precisamos das colunas de ID (que adicionamos ao SQL)
    if all(col in df.columns for col in ["id_categoria", "id_situacao", "co_protocolo_acervo"]):
        df["ic_registro_completo"] = (
            df["id_categoria"].notna() &
            df["id_situacao"].notna() &
            df["co_protocolo_acervo"].notna()
        )
        
    df["ic_tem_descritores"] = (
        df.get("id_descritor_informacional").notna() |
        df.get("id_descritor_catalografico").notna() |
        df.get("id_descritor_tecnico").notna()
    )
    df["ic_tem_iconografico"] = (
        df.get("id_descritoes_informacionais_iconografico").notna() |
        df.get("id_descritor_catalografico_iconografico").notna()
    )
    
    if "id_arquivo_acervo_pai" in df.columns:
        df["ic_tem_arquivo_pai"] = df["id_arquivo_acervo_pai"].notna()
    
    if "dt_emissao_etiqueta" in df.columns:
        df["nu_ano_emissao_etiqueta"] = df["dt_emissao_etiqueta"].dt.year.astype("Int64")
    
    # 3. Ordenação
    # Ordena pelo id_arquivo_acervo (numérico) que é garantido
    sort_cols = []
    if "id_categoria" in df.columns: sort_cols.append("id_categoria")
    if "co_protocolo_acervo" in df.columns: sort_cols.append("co_protocolo_acervo")
    if "id_arquivo_acervo" in df.columns: sort_cols.append("id_arquivo_acervo")
    
    if sort_cols:
        df = df.sort_values(by=sort_cols, na_position="last").reset_index(drop=True)
    
    # 4. Sanitização
    df = sanitize_dataframe(df)
    
    print(f"✅ Dados estruturados: {len(df):,} linhas, {len(df.columns)} colunas")
    return df


def save_file(df: pd.DataFrame, output_path: Path, format: str = "excel") -> Path:
    """Salva DataFrame em arquivo (Excel ou CSV)."""
    print(f"\n💾 Salvando arquivo: {output_path.name}")
    
    if format == "excel":
        df.to_excel(output_path, index=False, engine="openpyxl")
    else:
        df.to_csv(output_path, sep=";", index=False, encoding="utf-8-sig")
    
    file_size = output_path.stat().st_size / (1024 * 1024)
    print(f"✅ Arquivo salvo: {file_size:.2f} MB")
    return output_path


def send_email(file_path: Path, num_rows: int, num_cols: int, nextcloud_info: Dict[str, Optional[str]]) -> bool:
    """Envia email com anexo e informações do Nextcloud."""
    print(f"\n📧 Preparando email para: {', '.join(MAIL_TO)}")
    
    try:
        msg = EmailMessage()
        msg["Subject"] = f"Relatório tb_arquivo_acervo - {datetime.now().strftime('%d/%m/%Y %H:%M')}"
        msg["From"] = MAIL_FROM_HEADER
        msg["To"] = ", ".join(MAIL_TO)
        msg["Reply-To"] = MAIL_REPLY_TO
        msg["Date"] = formatdate(localtime=True)
        msg["Message-ID"] = make_msgid(domain=MAIL_EHLO_HOST)
        
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        
        # Corpo do email
        body_lines = [
            "Prezados,",
            "",
            "Segue em anexo o relatório do acervo de memória da SPU, extraído e processado pelo ETL automático.",
            "",
            "📊 Estatísticas:",
            f"- Linhas processadas: {num_rows:,}",
            f"- Colunas: {num_cols}",
            f"- Tamanho do arquivo: {file_size_mb:.2f} MB",
            f"- Arquivo: {file_path.name}",
            f"- Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}",
            "",
        ]
        
        # Adiciona informações do Nextcloud se disponível
        if nextcloud_info.get("share_url"):
            body_lines.extend([
                "☁️  Nextcloud:",
                f"- Publicado em: /{nextcloud_info.get('remote_path', '')}",
                f"- Link público: {nextcloud_info.get('share_url')}",
                f"- Download direto: {nextcloud_info.get('direct_download_url')}",
            ])
            if NC_SHARE_PASSWORD:
                body_lines.append(f"- Senha do link: {NC_SHARE_PASSWORD}")
            if NC_SHARE_EXPIRE_DAYS > 0:
                body_lines.append(f"- Expira em: {NC_SHARE_EXPIRE_DAYS} dia(s)")
            body_lines.append("")
        
        body_lines.extend([
            "Atenciosamente,",
            "Sistema SPIUnet - ETL Automático",
        ])
        
        msg.set_content("\n".join(body_lines))
        
        # Anexo
        print(f"📎 Anexando: {file_path.name}")
        with open(file_path, "rb") as f:
            if file_path.suffix == ".xlsx":
                maintype, subtype = "application", "vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            else:
                maintype, subtype = "text", "csv"
            msg.add_attachment(
                f.read(),
                maintype=maintype,
                subtype=subtype,
                filename=file_path.name,
            )
        
        # Envio
        print(f"🚀 Enviando via {MAIL_SERVER}:{MAIL_PORT}...")
        with smtplib.SMTP(MAIL_SERVER, MAIL_PORT, timeout=60) as smtp:
            smtp.set_debuglevel(0)
            smtp.ehlo(MAIL_EHLO_HOST)
            smtp.send_message(msg, from_addr=MAIL_FROM_ENVELOPE, to_addrs=MAIL_TO)
        
        print("✅ Email enviado com sucesso!")
        return True
    
    except socket.gaierror:
        print(f"❌ Erro de DNS: Não foi possível resolver o nome do servidor de email '{MAIL_SERVER}'.")
        print("   Verifique se o nome do servidor está correto e acessível.")
        print("   Você pode configurar um servidor diferente usando a variável de ambiente 'MAIL_SERVER'.")
        return False
    except Exception as e:
        print(f"❌ Erro ao enviar email: {e}")
        return False


def executar_pipeline_etl():
    """Executa o pipeline ETL completo do acervo."""
    start_time = datetime.now()
    
    print("=" * 80)
    print("🚀 ETL COMPLETO - tb_arquivo_acervo")
    print("=" * 80)
    print(f"⏰ Início: {start_time.strftime('%d/%m/%Y %H:%M:%S')}")
    print()
    
    try:
        # 1. Extração
        df = extract_data_from_impala()
        
        # 2. Estruturação
        df_structured = structure_data(df)
        
        # 3. Salvar arquivo (Excel para Nextcloud, CSV também)
        output_file = PROCESSED_DIR / OUTPUT_FILENAME
        save_file(df_structured, output_file, format="excel")
        
        # 4. Publicar no Nextcloud
        nextcloud_info = publish_to_nextcloud(output_file, OUTPUT_FILENAME)
        
        # 5. Enviar Email
        send_email(output_file, len(df_structured), len(df_structured.columns), nextcloud_info)
        
    except Exception as e:
        print(f"\n❌ Falha crítica no pipeline: {e}")
        sys.exit(1)
    
    end_time = datetime.now()
    duration = end_time - start_time
    print()
    print("=" * 80)
    print(f"🏁 Fim do ETL. Duração: {duration}")
    print("=" * 80)


if __name__ == "__main__":
    executar_pipeline_etl()

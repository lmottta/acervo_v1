-- =====================================================================
-- Script de Migração CSV - SPUNet Acervo para MemóriaSPU
-- VERSÃO PARA IMPALA (usando CONCAT_WS + COLLECT_LIST)
-- =====================================================================
-- Adaptações para Impala:
-- - GROUP_CONCAT → CONCAT_WS('||', COLLECT_LIST(...))
-- - Agregação de campos multivalorados
-- - Mesclagem de campos duplicados
-- - Transformações de FLAGS, tipos e valores vazios
-- =====================================================================

SELECT 
    -- ========== CAMPOS DA TABELA PRINCIPAL tb_arquivo_acervo ==========
    
    -- Campo 1: ic_principal (FLAG) - converter para Sim/Não
    CASE 
        WHEN ta.ic_principal = 1 OR ta.ic_principal = true THEN 'Sim'
        WHEN ta.ic_principal = 0 OR ta.ic_principal = false THEN 'Não'
        ELSE ''
    END AS ic_principal,
    
    -- Campo 2: id_arquivo_acervo - converter para STRING
    CAST(ta.id_arquivo_acervo AS STRING) AS id_arquivo_acervo,
    
    -- Campo 3: co_protocolo_sei - CORRIGIDO: agregar pois vem de tb_arquivo_anexo (1:N)
    COALESCE(CONCAT_WS('||', COLLECT_SET(tx.co_protocolo_sei)), '') AS co_protocolo_sei,
    
    -- Campo 4: co_protocolo_acervo
    COALESCE(ta.co_protocolo_acervo, '') AS co_protocolo_acervo,
    
    -- Campo 5: id_arquivo_acervo_pai - converter para STRING
    COALESCE(CAST(ta.id_arquivo_acervo_pai AS STRING), '') AS id_arquivo_acervo_pai,
    
    -- Campo 6: dt_emissao_etiqueta - converter para STRING formato data
    COALESCE(CAST(ta.dt_emissao_etiqueta AS STRING), '') AS dt_emissao_etiqueta,
    
    -- Campo 7: id_arquivo_acervo_1 (Arquivos associados) - IMPALA: CONCAT_WS + COLLECT_LIST
    COALESCE(CONCAT_WS('||', COLLECT_SET(CAST(tq.id_arquivo_acervo_1 AS STRING))), '') AS id_arquivo_acervo_1,
    
    -- Campo 8: id_arquivo_acervo_2 (Arquivos associados) - IMPALA: CONCAT_WS + COLLECT_LIST
    COALESCE(CONCAT_WS('||', COLLECT_SET(CAST(tq1.id_arquivo_acervo_2 AS STRING))), '') AS id_arquivo_acervo_2,
    
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
        WHEN td.ic_vinculado_spu = 1 OR td.ic_vinculado_spu = true THEN 'Sim'
        WHEN td.ic_vinculado_spu = 0 OR td.ic_vinculado_spu = false THEN 'Não'
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
    
    -- Campo 24: ds_autor_copia - IMPALA: CONCAT_WS + COLLECT_SET
    COALESCE(CONCAT_WS('||', COLLECT_SET(tac.ds_autor_copia)), '') AS ds_autor_copia,
    
    -- Campo 25: ds_autor_original - IMPALA: CONCAT_WS + COLLECT_SET
    COALESCE(CONCAT_WS('||', COLLECT_SET(tao.ds_autor_original)), '') AS ds_autor_original,
    
    -- Campo 26: ds_processo_vinculado - IMPALA: CONCAT_WS + COLLECT_SET
    COALESCE(CONCAT_WS('||', COLLECT_SET(tpv.ds_processo_vinculado)), '') AS ds_processo_vinculado,
    
    -- Campo 27: ds_palavra_chave_iconografico - IMPALA: CONCAT_WS + COLLECT_SET
    COALESCE(CONCAT_WS('||', COLLECT_SET(tpc.ds_palavra_chave)), '') AS ds_palavra_chave_iconografico,
    
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
        WHEN tdc.ic_iconografia = 1 OR tdc.ic_iconografia = true THEN 'Sim'
        WHEN tdc.ic_iconografia = 0 OR tdc.ic_iconografia = false THEN 'Não'
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
        WHEN tdc.ic_notacao_kardex = 1 OR tdc.ic_notacao_kardex = true THEN 'Sim'
        WHEN tdc.ic_notacao_kardex = 0 OR tdc.ic_notacao_kardex = false THEN 'Não'
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
    
    -- Campo 74: ds_interessado - IMPALA: CONCAT_WS + COLLECT_SET
    COALESCE(CONCAT_WS('||', COLLECT_SET(ti.ds_interessado)), '') AS ds_interessado,
    
    -- Campo 76: ds_processo_antecedente - IMPALA: CONCAT_WS + COLLECT_SET
    COALESCE(CONCAT_WS('||', COLLECT_SET(tpa.ds_processo_antecedente)), '') AS ds_processo_antecedente,
    
    -- Campo 77: ds_assunto
    COALESCE(tas.ds_assunto, '') AS ds_assunto,
    
    -- Campo 78: ds_especie_documental
    COALESCE(ted.ds_especie_documental, '') AS ds_especie_documental,
    
    -- Campo 80: ds_patrimonio
    COALESCE(tp.ds_patrimonio, '') AS ds_patrimonio,
    
    -- ========== CAMPOS ESPECIAIS ==========
    
    -- special_document - campo especial (vazio conforme documentação)
    '' AS special_document,
    
    -- special_attachments - URLs dos anexos agregados - IMPALA: CONCAT_WS + COLLECT_SET
    COALESCE(
        CONCAT_WS('||', COLLECT_SET(CONCAT('https://spunet.economia.gov.br/acervo/api/public/portal/arquivo-anexo/', CAST(tx.id_arquivo_anexo AS STRING),''))),
        ''
	) AS special_attachments

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
    ta.ic_publicado_internet = true
    AND (ta.id_situacao IS NULL OR ta.id_situacao != 2)

-- ========== GROUP BY (NECESSÁRIO para COLLECT_SET) ==========

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
    tp.ds_patrimonio

-- ========== ORDENAÇÃO ==========

ORDER BY ta.id_arquivo_acervo;

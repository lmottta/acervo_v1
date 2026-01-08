select 
    -- Dados Principais do Arquivo
    ta.ic_principal,
    ta.id_arquivo_acervo,
    tx.co_protocolo_sei,
    ta.co_protocolo_acervo,
    ta.id_arquivo_acervo_pai,
    ta.dt_emissao_etiqueta,
    -- Arquivos Associados
    tq.id_arquivo_acervo_1 as id_arquivo_associado_1,
    tq1.id_arquivo_acervo_2 as id_arquivo_associado_2,
    -- Categoria
    tc.ds_categoria,
    -- Descrições Informacionais Iconográficas (TD)
    td.nu_area_equivalente,
    td.nu_area_total,
    td.ds_bairro as icon_ds_bairro,
    td.ds_confrontacoes as icon_ds_confrontacoes,
    td.nu_escala_cartografica,
    td.ds_informacoes_complementares as icon_ds_informacoes_complementares,
    td.ds_logradouro_atualizado as icon_ds_logradouro_atualizado,
    td.ds_logradouro_original as icon_ds_logradouro_original,
    td.nu_area_total as nu_metro,
    td.nu_rip as icon_nu_rip,
    td.ic_vinculado_spu,
    td.ds_orgao_emissor as icon_ds_orgao_emissor,
    td.ds_regiao_administrativa,
    td.ds_tipo_documento,
    -- Autor Copia e Original
    tac.ds_autor_copia,
    tao.ds_autor_original,
    -- Processo Vinculado
    tpv.ds_processo_vinculado,
    -- Palavra Chave
    tpc.ds_palavra_chave,
    -- Localização e Tipo (Iconográfico)
    tm.ds_municipio,
    ttd.ds_tipo_documental,
    tuf.ds_unidade_federativa,
    tum.ds_unidade_medida,
    -- Descritor Catalográfico (TDC)
    tdc.ds_caixa,
    tdc.ds_estante,
    tdc.ic_iconografia,
    tdc.tx_informacoes_complementares as cat_tx_informacoes_complementares,
    tdc.ic_notacao_kardex,
    tdc.ic_especie_cartografia,
    tdc.ic_especie_fotografia,
    tdc.ds_prateleira,
    -- Condição e Dimensão
    tco.ds_condicao_original,
    tds.ds_dimensao_suporte,
    -- Descritor Catalográfico Iconográfico (TDCI)
    tdci.ds_armario,
    tdci.ds_gaveta,
    tdci.ds_info_complementares as cat_icon_ds_info_complementares,
    tdci.ds_mapoteca,
    tdci.ds_orgao_tutela,
    tdci.ds_orgao_origem as cat_icon_ds_orgao_origem,
    tdci.ds_planta,
    tdci.ds_prancha,
    tdci.ds_item,
    tdci.ds_tela,
    tdci.ds_tubo,
    -- Documento e Material
    tdoc.ds_documento, 
    tms.ds_material_suporte,
    -- Descrições Informacionais (TDI)
    tdi.ds_bairro as info_ds_bairro,
    tdi.ds_cep,
    tdi.ds_confrontacoes as info_ds_confrontacoes,
    tdi.dt_atuacao_processo,
    tdi.ds_denominacao_patrimonial,
    tdi.ds_info_complementares as info_ds_info_complementares,
    tdi.ds_logradouro_atualizado as info_ds_logradouro_atualizado,
    tdi.ds_logradouro_original as info_ds_logradouro_original,
    tdi.ds_processo_administrativo,
    tdi.ds_processo_outro_orgao,
    tdi.nu_rip as info_nu_rip,
    tdi.nu_rip_spiu,
    tdi.nu_rip_siapa,
    tdi.ds_orgao_emissor as info_ds_orgao_emissor,
    tdi.ds_pasta_requisito_ref,
    tdi.ds_titulo_formal,
    -- Interessado e Antecedente
    ti.ds_interessado,
    tpa.ds_processo_antecedente,
    -- Assunto, Especie, Patrimonio
    ts.ds_assunto,
    ted.ds_especie_documental,
    tp.ds_patrimonio
from tb_arquivo_acervo ta 
    left join tb_arquivo_anexo tx on tx.id_arquivo_acervo = ta.id_arquivo_acervo 
    left join tb_arquivo_acervo_associado tq on tq.id_arquivo_acervo_1 = ta.id_arquivo_acervo 
    left join tb_arquivo_acervo_associado tq1 on tq1.id_arquivo_acervo_2 = ta.id_arquivo_acervo
    left join tb_categoria tc on tc.id_categoria = ta.id_categoria
    -- Join Iconográfico
    left join tb_descritoes_informacionais_iconografico td on td.id_descritoes_informacionais_iconografico = ta.id_descritoes_informacionais_iconografico
    left join tb_autor_copia tac on tac.id_descritoes_informacionais_iconografico = td.id_descritoes_informacionais_iconografico 
    left join tb_autor_original tao on tao.id_descritoes_informacionais_iconografico = td.id_descritoes_informacionais_iconografico
    left join tb_processo_vinculado tpv on tpv.id_descritoes_informacionais_iconografico = td.id_descritoes_informacionais_iconografico
    -- Palavra Chave via Tabela Associativa
    left join tb_palavra_chave_descritoes_informacionais_iconografico tpcd on tpcd.id_descritoes_informacionais_iconografico = ta.id_descritoes_informacionais_iconografico
    left join tb_palavra_chave tpc on tpc.id_palavra_chave = tpcd.id_palavra_chave 
    -- Lookups Iconográfico
    left join tb_municipio tm on tm.id_municipio = td.id_municipio 
    left join tb_tipo_documental ttd on ttd.id_tipo_documental = td.id_tipo_documental
    left join tb_unidade_federativa tuf on tuf.id_unidade_federativa = td.id_unidade_federativa
    left join tb_unidade_medida tum on tum.id_unidade_medida = td.id_unidade_medida 
    -- Join Catalográfico
    left join tb_descritor_catalografico tdc on tdc.id_descritor_catalografico = ta.id_descritor_catalografico 
    left join tb_condicao_original tco on tco.id_condicao_original = tdc.id_condicao_original 
    left join tb_dimensao_suporte tds on tds.id_dimensao_suporte = tdc.id_dimensao_suporte 
    -- Join Catalográfico Iconográfico
    left join tb_descritor_catalografico_iconografico tdci on tdci.id_descritor_catalografico_iconografico = ta.id_descritor_catalografico_iconografico
    left join tb_documento tdoc on tdoc.id_documento = tdci.id_documento 
    left join tb_material_suporte tms on tms.id_material_suporte = tdci.id_material_suporte 
    -- Join Descrições Informacionais
    left join tb_descritoes_informacionais tdi on tdi.id_descritor_informacional = ta.id_descritor_informacional
    left join tb_interessado ti on ti.id_descritor_informacional = tdi.id_descritor_informacional
    left join tb_processo_antecedente tpa on tpa.id_descritor_informacional = tdi.id_descritor_informacional
    left join tb_assunto ts on ts.id_assunto = tdi.co_assunto 
    left join tb_especie_documental ted on ted.id_especie_documental = tdi.id_especie_documental 
    left join tb_patrimonio tp on tp.id_patrimonio = tdi.id_patrimonio;

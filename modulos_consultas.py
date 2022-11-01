from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd
import time
import datetime
from datetime import datetime



print(datetime.today().strftime('%H:%M'))
start_time = time.time()

def backlog():
        #redshift_endpoint1 = "madeiramadeira.cfo4lisnugyt.us-east-2.redshift.amazonaws.com"
        redshift_endpoint1 = "redshift.madeiramadeira.com.br"
        redshift_user1 = "eliana.rodrigues"
        redshift_pass1 = "pnAwDo9bMmLqggbA"
        port1 = 5439
        dbname1 = "wood"

        engine_string = "postgresql+psycopg2://%s:%s@%s:%d/%s" \
        % (redshift_user1, redshift_pass1, redshift_endpoint1, port1, dbname1)
        engine1 = create_engine(engine_string)

        sql1 = """
        WITH base as (SELECT *
        FROM bases_bulkylog.base_backlog)
        SELECT
        UPPER(base.origem + cidade_cliente + estado_cliente + transp_entrega) AS chaveorigem,
        (base.pedido + sku) AS pedidosku,
        nf_madeira                  AS "01. NF Madeira",
        pedido                      AS "pedido",
        oc                          AS "03. OC",
        nf_fornecedor               AS "04. NF Fornecedor",
        fornecedor                  AS "05. Fornecedor",
        sku                         AS "07. Codigo SKU",
        base.valor_frete            AS "08. Valor Frete",
        valor_nf_cliente            AS "09. Valor Total NF",
        peso_cubado_nf              AS "10. Peso NF (kg)",
        volumes_nf                  AS "11. Qntd. Volumes NF",
        nome_cliente                AS "12. Nome Cliente",
        telefone_cliente            AS "13. Telefone Cliente",
        fornecedor_cnpj             AS "14. CNPJ Fornecedor",
        base.origem                      AS "15. Origem",
        destino_cliente             AS "16. Destino",
        cep_cliente                 AS "17. CEP",
        base.market_place                AS "18. Market Place",
        analista_t2                 AS "19. Analista T2",
        analista_t3                 AS "20. Analista T3",
        transp_redespacho_nf        AS "21. Transp. Redespacho NF",
        transp_redespacho           AS "22. Transp. Redespacho",
        transp_entrega_nf           AS "23. Transp. Entrega NF",
        transp_entrega              AS "24. Transp. Entrega",
        (case
            when data_limite_cliente_item = '1900-01-01 00:00:00.000' then NULL
            Else date(data_limite_cliente_item) end) as "25. Data Limite Cliente",
        (case
            when data_limite_fornecedor = '1900-01-01 00:00:00.000' then NULL
            Else date(data_limite_fornecedor) end) as "26. Data Limite Fornecedor",
        (case
            when data_limite_coleta = '1900-01-01 00:00:00.000' then NULL
            Else date(data_limite_coleta) end) as "27. Data Limite Coleta",
        (case
            when data_limite_transporte = '01/01/1900' then NULL
            Else date(data_limite_transporte) end) as "28. Data Limite Transporte",
        (case
            when data_limite_redespacho = '1900-01-01 00:00:00.000' then NULL
            Else date(data_limite_redespacho) end) as "29. Data Limite Redespacho",
        prazo_redespacho_novo       AS "30. Prazo Redespacho",
        base.prazo_transportadora        AS "32. Prazo Last Mile",
        (case
            when base.data_compra = '1900-01-01 00:00:00.000' then NULL
            Else date(base.data_compra) end) as "37. Data Compra Cliente",
        (case
            when data_criacao_oc = '1900-01-01 00:00:00.000' then NULL
            Else date(data_criacao_oc) end) as "38. Data Criacao OC",
        (case
            when data_aceite_oc = '1900-01-01 00:00:00.000' then NULL
            Else date(data_aceite_oc) end) as "39. Data Aceite OC",
        (case
            when base.data_faturamento = '1900-01-01 00:00:00.000' then NULL
            Else date(base.data_faturamento) end) as "40. Data Faturamento NF MI",
        (case
            when data_bip = '1900-01-01 00:00:00.000' then NULL
            Else date(data_bip) end) as "41. Data BIP",
        (case
            when data_expedicao_fornecedor = '1900-01-01 00:00:00.000' then NULL
            Else date(data_expedicao_fornecedor) end) as "42. Data Expedicao",
        transp_ultimo_edi           AS "43. Transp. Ultimo EDI",
        ultimo_edi                  AS "44. Ultimo EDI",
        observacao_ultimo_edi       AS "45.Observacao Ultimo EDI",
        (case
            when data_ultimo_edi = '1900-01-01 00:00:00.000' then NULL
            Else date(data_ultimo_edi) end) as "46. Data Ultimo EDI",
        (case
            when data_ctrc_emitido_redespacho = '1900-01-01 00:00:00.000' then NULL
            Else date(data_ctrc_emitido_redespacho) end) as "48. Data CTRC Emitido T2",
        (case
            when data_saida_origem_redespacho = '1900-01-01 00:00:00.000' then NULL
            Else date(data_saida_origem_redespacho) end) as "49. Data Saida Origem T2",
        (case
            when data_chegada_destino_redespacho = '1900-01-01 00:00:00.000' then NULL
            Else date(data_chegada_destino_redespacho) end) as "50. Data Chegada Destino T2",
        (case
            when data_em_rota_entrega_redespacho = '1900-01-01 00:00:00.000' then NULL
            Else date(data_em_rota_entrega_redespacho) end) as "51. Data Em Rota de Entrega T2",
        (case
            when data_redespacho = '1900-01-01 00:00:00.000' then NULL
            Else date(data_redespacho) end) as "52. Data Redespacho",
        (case
            when data_movimentacao_last_mile = '1900-01-01 00:00:00.000' then NULL
            Else date(data_movimentacao_last_mile) end) as "53. Data Movimentacao T3",
        (case
            when data_ctrc_emitido_last_mile = '1900-01-01 00:00:00.000' then NULL
            Else date(data_ctrc_emitido_last_mile) end) as "54. Data CTRC Emitido T3",
        (case
            when data_saida_origem_last_mile = '1900-01-01 00:00:00.000' then NULL
            Else date(data_saida_origem_last_mile) end) as "55. Data Saida Origem T3",
        (case
            when data_chegada_destino_last_mile = '1900-01-01 00:00:00.000' then NULL
            Else date(data_chegada_destino_last_mile) end) as "56. Data Chegada Destino T3",
        (case
            when data_em_rota_entrega_last_mile = '1900-01-01 00:00:00.000' then NULL
            Else date(data_em_rota_entrega_last_mile) end) as "57. Data Em Rota de Entrega T3",
        (case
            when base.data_entrega = '1900-01-01 00:00:00.000' then NULL
            Else date(base.data_entrega) end) as "58. Data Entrega",
        tipo_avaria                 AS "63. Tipo Avaria",
        data_apontamento_avaria     AS "64. Data Apontamento Avaria",
        (case
            when data_solicitacao_devolucao = '1900-01-01 00:00:00.000' then NULL
            Else date(data_solicitacao_devolucao) end) as "66. Data Solicitacao Devolucao",
        data_analise_devolucao      AS "67. Data Analise Devolucao",
        (case
            when data_solicitacao_contato = '1900-01-01 00:00:00.000' then NULL
            Else date(data_solicitacao_contato) end) as "68. Data Solicitacao Contato",
        (case
            when data_retorno_contato = '1900-01-01 00:00:00.000' then NULL
            Else date(data_retorno_contato) end) as "70. Data Retorno Contato",
        ultimo_sac_aberto           AS "72. Qual o Ultimo SAC aberto?",
        status_retorno_contato      AS "74. Status Retorno Contato",
        danfe_nf_fornecedor         AS "75. Danfe_Nf_Fornecedor",
        danfe_nf_madeira            AS "76. Danfe_Nf_Mi",
        (case
            when data_criacao_pv = '1900-01-01 00:00:00.000' then NULL
            Else date(data_criacao_pv) end) as "77. Data_Criacao_Pv",
        (case
            when data_revisada_cliente = '1900-01-01 00:00:00.000' then NULL
            Else date(data_revisada_cliente) end) as "78. Data_Revisada_Cliente",
        market_place_grupo          AS "79. Market_Place_Grupo",
        pedido_em_guideshop         AS "81. Pedido Em Guideshop",
        base.prazo_fornecedor            AS "82. Prazo_Fornecedor",
        status_eagle                AS "83. Status_Eagle",
        status_portal               AS "84. Status_Portal",
        estado_cliente              AS "87. UF Destino",
        cidade_cliente              AS "88. Cidade",
        direto_status               AS "Direto?",
        quantidade_confirmada_oc    AS "quantidade_confirmada_oc",
        id_origem                   AS "id_origem",
        (Case When direto_status = 'Direto' Then 'Direto'
              When transp_redespacho = transp_entrega Then 'Direto'
              else 'Redespacho' End) AS "Tipo De Transporte",
        (case
            when data_aceite_nf_cd = '1900-01-01 00:00:00.000' then NULL
            Else date(data_aceite_nf_cd) end) as "data_aceite_nf_cd",
        (Case when base.origem_reserva <> ''  THEN 'ESTOQUE MADEIRA'
        ELSE 'OUTROS' END)         AS "CROSS/ENTREGA/VAO",
         base.origem_reserva,
         pipv.codigo_reserva,
         pipc.idfk_pedido_compra,
         pipc.idfk_armazem,
         pipv.origem_reserva,
         pt.nome_reduzido,
         ptc.unidade_codigo,
         ls.tipo as cabecasa,
         ls.loja as guide,
         ls.tipo_pagamento_1 as forma_pagamento,
         ppc.descricao as "categoria",
         ppv.recompra,
         (case when pp.idfk_tipo_assistencia is not null then
            'Assistencia'
        else
            ''
        end) as assistencia,
        (case
            when pipc.idfk_pedido_compra <> '' and pipv.codigo_reserva is null and pipc.idfk_armazem is null then 'VAO'
            when pipv.codigo_reserva is not null and pipc.idfk_pedido_compra = '' or pipc.idfk_pedido_compra is null then 'CD'
            when pipc.idfk_pedido_compra <> '' and pipc.idfk_armazem is not null and pipv.codigo_reserva is null then 'CROSS'
            when pipc.idfk_pedido_compra <> '' and pipc.idfk_armazem is not null and pipv.codigo_reserva is not null and pipv.origem_reserva is not null then 'CROSS'
            when pipv.codigo_reserva is not null and pipc.idfk_pedido_compra <> '' then 'FF > VAO'
        else null	end) as  "tipo_venda",
        (case 
        	when ppv.recompra = 1 then
			(select date(pipv.data_entrega) 
			from lake_brain.portal_item_pedido_venda pipv  
			where pipv.idfk_pedido_venda = concat('Z',REGEXP_REPLACE(base.pedido,'[A-Z]', '')) and pipv.codigo_sku = base.sku )			
		else 
			NULL		 
					end) as "data_limite_cliente_recompra"
        FROM base
            left join
            lake_brain.portal_pedido_venda ppv on ppv.id = base.pedido
            left join
            lake_brain.portal_item_pedido_venda pipv on pipv.idfk_pedido_venda = base.pedido and pipv.codigo_sku = base.sku 
            left join
            lake_brain.portal_item_pedido_compra pipc on pipc.idfk_item_pedido_venda = pipv.id and pipc.idfk_pedido_compra = base.oc
            left join
            lake_brain.portal_transportadora_coleta ptc on ptc.numero_pedido_venda = base.pedido AND ptc.idfk_faturamento = base.id_faturamento
            left join
            lake_brain.portal_transportadora pt on pt.id = ptc.idfk_transportadora_filial_parceiro
            left join
            lake_brain.portal_faturamento fat on fat.idfk_pedido_venda = ptc.numero_pedido_venda
            left join
            lake_brain.portal_transportadora_nota_fiscal_madeira nfm on nfm.idfk_faturamento = fat.id
            left join
            looker_plan.looker_sales ls on ls.id_pedido = REGEXP_REPLACE(base.pedido, '[A-Z]','') and ls.id_produto = base.sku
            left join lake_brain.portal_produto pp on pp.id = base.sku
            left join lake_brain.portal_produto_categoria ppc ON  ppc.id = pp.idfk_categoria 
        """
        df_red = pd.read_sql_query(text(sql1), engine1)
        print(datetime.today().strftime('%H:%M'))
        print('Query - Ok', " - %s" % (time.time() - start_time), " - segundos")
        print(df_red)
        return df_red

def prazo():
    # redshift_endpoint1 = "madeiramadeira.cfo4lisnugyt.us-east-2.redshift.amazonaws.com"
    redshift_endpoint1 = "redshift.madeiramadeira.com.br"
    redshift_user1 = "eliana.rodrigues"
    redshift_pass1 = "pnAwDo9bMmLqggbA"
    port1 = 5439
    dbname1 = "wood"

    engine_string = "postgresql+psycopg2://%s:%s@%s:%d/%s" \
                    % (redshift_user1, redshift_pass1, redshift_endpoint1, port1, dbname1)
    engine3 = create_engine(engine_string)
    sql3 = """
    select
    	UPPER(pcr.origem + pcr.cidade + pcr.uf + pcr.transp_t3) AS chaveorigem,
        pcr.prazo_redespacho,
        pcr.datetime_cadastro
    FROM
        lake_bases_bulkylog.prazos_cadastrados_redespacho pcr 
        WHERE pcr.datetime_cadastro = (SELECT MAX(pcr.datetime_cadastro) FROM lake_bases_bulkylog.prazos_cadastrados_redespacho pcr )
    """
    prazo = pd.read_sql_query(text(sql3), engine3)

    return prazo

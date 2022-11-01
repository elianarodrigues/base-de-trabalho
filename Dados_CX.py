import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import text
import pymysql
import pandas as pd
from datetime import  datetime
import time

print(datetime.today().strftime('%H:%M'))
start_time = time.time()

user = 'eliana.rodrigues'
senha = 'wpRv%miB#zZlmbFvXW'
engine_string = 'mysql+pymysql://%s:%s@portal-ro3.madeiramadeira.com.br:3306/brain' % (user,senha)
engine_solicitacao = sqlalchemy.create_engine(engine_string)
query ='''
select 
pcie.idfk_pedido as Pedido,
nfm.numero as NF_MI,
pcie.idfk_status,
CONCAT(pcie.idfk_pedido, nfm.numero) AS Chave
from 
	portal_item_pedido_venda ipv
		INNER JOIN
    portal_pedido_venda pv ON ipv.idfk_pedido_venda = pv.id and pv.idfk_pedido_venda_origem <> 2 #Retira pedidos com origem SELLER
		LEFT JOIN
    portal_item_faturamento pif ON ipv.id = pif.idfk_item_pedido_venda
		LEFT JOIN
    portal_faturamento fat ON fat.id = pif.idfk_faturamento
		LEFT JOIN
	portal_transportadora_nota_fiscal_madeira nfm ON nfm.idfk_faturamento = fat.id
		LEFT JOIN
	portal_item_pedido_venda_status_fc fc ON ipv.novo_status_fc = fc.id
		LEFT JOIN
	portal_cliente_insucesso_entrega pcie on pv.id = pcie.idfk_pedido
		LEFT JOIN
	portal_cliente_insucesso_entrega_status pcies on pcie.idfk_status = pcies.id
where 
	pcie.idfk_status IN (1, 2, 3)
    AND date(pcie.created) >= '20210701'
    AND fc.descricao = 'REMETIDO'
;

'''
solicitacao = pd.read_sql_query(query,engine_solicitacao)
dados_cx = solicitacao.copy()
del solicitacao

dados_cx.to_csv(r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\Dados_CX.csv', sep=';', index=False)
print(dados_cx.head())

print(datetime.today().strftime('%H:%M'))
start_time = time.time()
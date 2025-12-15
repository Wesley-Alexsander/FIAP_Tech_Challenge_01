from dados import PipelineDadosVinho
from pprint import pprint

pipeline = PipelineDadosVinho(densidade_vm=0.995)
dados_por_ano = pipeline.executar_pipeline(2009, 2024)
dados_unificados = pipeline.unificar_dados(dados_por_ano)
print(dados_unificados)
metadados = pipeline.obter_metadados(dados_por_ano)
pprint(metadados)
print(dados_unificados)


pipeline.salvar_dados_separados(dados_por_ano)
pipeline.salvar_dados_unificados(dados_unificados)
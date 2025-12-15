import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from decimal import Decimal

class PipelineDadosVinho:
    """Pipeline para processamento de dados de exportação de vinho do Brasil"""
    
    def __init__(self, densidade_vm: float = 0.995):
        """
        Inicializa a pipeline
        
        Args:
            densidade_vm: Densidade do vinho para conversão kg → litros
        """
        self.densidade_vm = densidade_vm
        self.vitibrasil_url = 'http://vitibrasil.cnpuv.embrapa.br/'
        self.cambio_url = 'https://www.dineroeneltiempo.com/divisas/usd-brl/historico?utm_source=chatgpt.com'
        self.continentes_url = "https://paintmaps.com/pt/informacoes-do-pais/continente"
        
        # Dicionário para correção de nomes de países
        self.replacement_dict = {
            'Alemanha, República Democrática': 'Alemanha',
            'Cayman, Ilhas': 'Ilhas Cayman',
            'Cocos (Keeling), Ilhas': 'Ilhas Cocos',
            'Eslovaca, Republica': 'Eslováquia',
            'Marshall, Ilhas': 'Ilhas Marshall',
            'Tcheca, República': 'República Tcheca',
            'Taiwan (FORMOSA)': 'Taiwan',
            'Coreia, Republica Sul': 'Coréia do Sul',
            'Taiwan (Formosa)': 'Taiwan'
        }
        
        # Cache para dados baixados
        self._dados_cambio = None
        self._dados_continentes = None
        
    def baixar_dados_cambio(self) -> pd.DataFrame:
        """Baixa e processa dados históricos de câmbio USD/BRL"""
        if self._dados_cambio is not None:
            return self._dados_cambio.copy()
        
        replacement_labels = {
            'Año': 'Ano',
            'Precio Cierre': 'Preco_fechamento',
            'Cambio %': 'Cambio%',
            'Promedio': 'Cambio',
            'Mínimo': 'Minimo',
            'Máximo': 'Maximo'
        }
        
        cambio_df = pd.read_html(
            self.cambio_url, 
            encoding='UTF-8'
        )[0]
        
        cambio_df.rename(columns=replacement_labels, inplace=True)
        
        # Converter colunas numéricas
        colunas_para_arredondar = ['Preco_fechamento', 'Cambio', 'Minimo', 'Maximo']
        for coluna in colunas_para_arredondar:
            cambio_df[coluna] = pd.to_numeric(
                cambio_df[coluna].astype(str).str.replace(',', '.'), 
                errors='coerce'
            ).round(2)
        
        self._dados_cambio = cambio_df
        return cambio_df.copy()
    
    def baixar_dados_continentes(self) -> pd.DataFrame:
        """Baixa e processa dados de continentes por país"""
        if self._dados_continentes is not None:
            return self._dados_continentes.copy()
        
        continentes_df = pd.read_html(
            self.continentes_url, 
            encoding="UTF-8"
        )[0]
        
        # Mapeamento especial para alguns países
        mapa_continente_especial = {
            'Rússia': 'Europa',
            'Turquia': 'Ásia',
            'Cazaquistão': 'Ásia',
        }
        
        continentes_df['CONTINENTE'] = (
            continentes_df['PAÍS']
            .map(mapa_continente_especial)
            .fillna(continentes_df['CONTINENTE'])
        )
        
        self._dados_continentes = continentes_df
        return continentes_df.copy()
    
    def baixar_dados_exportacao_ano(self, ano: int) -> pd.DataFrame:
        """
        Baixa dados de exportação para um ano específico
        
        Args:
            ano: Ano dos dados a serem baixados
            
        Returns:
            DataFrame com dados de exportação do ano
        """
        url = f'{self.vitibrasil_url}/index.php?ano={ano}&opcao=opt_06&subopcao=subopt_01'
        df = pd.read_html(url, encoding='UTF-8')[3]
        
        # Remover linhas com NaN e total
        df.dropna(inplace=True)
        df = df.loc[df['Países'] != 'Total']
        
        # Adicionar coluna de ano
        df['Ano'] = ano
        
        return df
    
    def processar_dados_exportacao(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Processa e enriquece os dados de exportação
        
        Args:
            df: DataFrame com dados brutos de exportação
            
        Returns:
            DataFrame processado e enriquecido
        """
        df_processado = df.copy()
        
        # Baixar dados auxiliares
        cambio_df = self.baixar_dados_cambio()
        continentes_df = self.baixar_dados_continentes()
        
        # Merge com dados de câmbio
        df_processado = pd.merge(
            df_processado, 
            cambio_df, 
            on='Ano', 
            how='left'
        )
        
        # Tratamento de valores
        df_processado = self._tratar_valores(df_processado)
        
        # Criar novas variáveis
        df_processado = self._criar_variaveis(df_processado)
        
        # Adicionar informações de continente
        df_processado = pd.merge(
            df_processado,
            continentes_df,
            left_on='Países',
            right_on='PAÍS',
            how='left'
        )
        
        # Calcular market share
        total_valor_usd = df_processado['Valor (US$)'].sum()
        df_processado['Market_Share'] = (
            (df_processado['Valor (US$)'] / total_valor_usd * 100)
            .round(2)
        )
        
        # Categorizar volume
        df_processado['Quantidade_Volume'] = self._categorizar_volume(
            df_processado['Quantidade (L)']
        )
        
        # Remover coluna auxiliar
        if 'PAÍS' in df_processado.columns:
            df_processado.drop('PAÍS', axis=1, inplace=True)
        
        return df_processado
    
    def _tratar_valores(self, df: pd.DataFrame) -> pd.DataFrame:
        """Trata valores das colunas Quantidade e Valor"""
        df_tratado = df.copy()
        
        # Tratar Quantidade (Kg)
        df_tratado['Quantidade (Kg)'] = (
            df_tratado['Quantidade (Kg)']
            .astype(str)
            .str.strip()
            .replace('-', '0')
            .str.replace('.', '', regex=False)
        )
        
        df_tratado['Quantidade (Kg)'] = pd.to_numeric(
            df_tratado['Quantidade (Kg)'],
            errors='coerce'
        ).astype('Int64')
        
        # Tratar Valor (US$)
        df_tratado['Valor (US$)'] = (
            df_tratado['Valor (US$)']
            .astype(str)
            .str.strip()
            .replace('-', '0')
            .str.replace('.', '', regex=False)
        )
        
        df_tratado['Valor (US$)'] = pd.to_numeric(
            df_tratado['Valor (US$)'],
            errors='coerce'
        ).astype('Float64')
        
        # Corrigir nomes de países
        df_tratado['Países'] = df_tratado['Países'].replace(self.replacement_dict)
        
        return df_tratado
    
    def _criar_variaveis(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cria novas variáveis derivadas"""
        df_enriquecido = df.copy()
        
        # Quantidade em litros
        df_enriquecido['Quantidade (L)'] = (
            df_enriquecido['Quantidade (Kg)'] / self.densidade_vm
        ).round(2)
        
        # Valor em Reais
        df_enriquecido['Valor (R$)'] = (
            df_enriquecido['Valor (US$)'] * df_enriquecido['Cambio']
        ).round(2)
        
        # Valor por litro em US$
        df_enriquecido['Valor (L) US$'] = (
            df_enriquecido['Valor (US$)'] / df_enriquecido['Quantidade (L)']
        ).where(df_enriquecido['Quantidade (L)'] > 0, 0).round(2)
        
        # Valor por litro em R$
        df_enriquecido['Valor (L) R$'] = (
            df_enriquecido['Valor (R$)'] / df_enriquecido['Quantidade (L)']
        ).where(df_enriquecido['Quantidade (L)'] > 0, 0).round(2)
        
        return df_enriquecido
    
    def _categorizar_volume(self, serie_volume: pd.Series) -> pd.Series:
        """Categoriza volumes em níveis"""
        volume_categorizado = pd.Series(
            'Sem Volume', 
            index=serie_volume.index
        )
        
        # Apenas categorizar volumes maiores que 0
        mask = serie_volume > 0
        
        if mask.any():
            volumes_positivos = serie_volume[mask]
            
            # Usar quartis para categorização
            try:
                categorias = pd.qcut(
                    volumes_positivos,
                    q=4,
                    labels=['Muito Baixo', 'Baixo', 'Médio', 'Alto']
                )
                volume_categorizado[mask] = categorias
            except ValueError:
                # Se não for possível usar quartis (poucos valores únicos)
                # Usar categorização simples
                bins = pd.cut(
                    volumes_positivos,
                    bins=4,
                    labels=['Muito Baixo', 'Baixo', 'Médio', 'Alto']
                )
                volume_categorizado[mask] = bins
        
        return volume_categorizado
    
    def executar_pipeline(self, ano_inicio: int, ano_fim: int) -> Dict[str, pd.DataFrame]:
        """
        Executa pipeline completa para um período
        
        Args:
            ano_inicio: Ano inicial do período
            ano_fim: Ano final do período
            
        Returns:
            Dicionário com DataFrames por ano
        """
        dados_por_ano = {}
        
        for ano in range(ano_inicio, ano_fim + 1):
            print(f"Processando ano {ano}...")
            
            # Baixar dados brutos
            dados_brutos = self.baixar_dados_exportacao_ano(ano)
            
            # Processar dados
            dados_processados = self.processar_dados_exportacao(dados_brutos)
            
            # Armazenar
            dados_por_ano[f'exp_{ano}'] = dados_processados
        
        return dados_por_ano
    
    def unificar_dados(self, dados_por_ano: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Unifica dados de múltiplos anos em um único DataFrame
        
        Args:
            dados_por_ano: Dicionário com DataFrames por ano
            
        Returns:
            DataFrame unificado
        """
        todos_dados = []
        
        for ano, df in dados_por_ano.items():
            todos_dados.append(df)
        
        return pd.concat(todos_dados, ignore_index=True)
    
    def obter_metadados(self, dados_por_ano: Dict[str, pd.DataFrame]) -> Dict:
        """
        Obtém metadados sobre os dados processados
        
        Args:
            dados_por_ano: Dicionário com DataFrames por ano
            
        Returns:
            Dicionário com metadados
        """
        primeiro_ano = list(dados_por_ano.keys())[0]
        df_primeiro_ano = dados_por_ano[primeiro_ano]
        
        metadados = {
            'total_paises': len(df_primeiro_ano['Países'].unique()),
            'colunas': list(df_primeiro_ano.columns),
            'total_anos': len(dados_por_ano),
            'anos_disponiveis': list(dados_por_ano.keys()),
            'estrutura_dados': {
                'colunas_numericas': df_primeiro_ano.select_dtypes(include=[np.number]).columns.tolist(),
                'colunas_categoricas': df_primeiro_ano.select_dtypes(include=['object']).columns.tolist(),
            }
        }
        
        return metadados

    def salvar_dados_separados(self, dados_por_ano: Dict[str, pd.DataFrame]):
        """
        Salva os DataFrames em CSV
        
        Args:
            dados_por_ano: Dicionário com DataFrames por ano
            
        Returns:
            Dicionário com metadados
        """
        for nome, tabela in dados_por_ano.items():
            tabela.to_csv(f'Data/Embrapa_vitibrasil_{nome}.csv', encoding='UTF-8', index=False)

        return f'dados salvos'

    def salvar_dados_unificados(self, dados_unificados: pd.DataFrame) -> str:
        """
        Salva os DataFrames em CSV
        
        Args:
            dados_unificados: DataFrame
            
        Returns:
            String
        """
        dados_unificados.to_csv('Data/Empraba_vitibrasil_exp.csv', encoding='UTF-8', index=False)
        return f'dados salvos'


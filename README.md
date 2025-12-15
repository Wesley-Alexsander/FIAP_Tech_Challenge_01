# 📊 FIAP – Tech Challenge 01 | Data Science  
## Análise, Tratamento de Dados e Storytelling com Dados da Vitivinicultura Brasileira

[![Licença](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)  
[![Status do Projeto](https://img.shields.io/badge/status-em%20desenvolvimento-brightgreen)]()

Este repositório contém o desenvolvimento do **Tech Challenge 01** do curso de **Data Science da FIAP**, cujo objetivo é aplicar, na prática, os fundamentos de **tratamento de dados, análise exploratória e construção de storytelling**, utilizando dados reais da base **Embrapa – VitiBrasil**.

<p align="center">
  <img src="./Assets/img/vitibrasil.png" alt="Capa da página Embrapa VitiBrasil" width="800"/>
</p>

> *“Data Science não é apenas sobre modelos, mas sobre transformar dados em decisões.”*

---

## 🎯 Objetivo do Projeto

O objetivo deste Tech Challenge é:

- Realizar o **tratamento e limpeza de dados reais**
- Aplicar **análise exploratória de dados (EDA)**
- Construir **indicadores analíticos relevantes**
- Desenvolver uma **narrativa clara e coerente (data storytelling)** baseada nos dados
- Traduzir números em **insights compreensíveis e acionáveis**

---

## 🧠 Contexto do Problema

A base **Embrapa VitiBrasil** reúne informações históricas sobre a produção, comercialização e exportação de vinhos e derivados no Brasil.

O desafio consiste em:
- Compreender a estrutura dos dados
- Corrigir inconsistências
- Padronizar métricas
- Analisar tendências ao longo do tempo
- Avaliar participação de mercado, volumes e valores
- Contar a história por trás dos dados

---

## 🔬 Abordagem Metodológica

O projeto foi desenvolvido seguindo as etapas clássicas de um pipeline de Data Science:

### 1️⃣ Entendimento dos Dados
- Análise da origem e do contexto da base
- Avaliação de colunas, tipos e granularidade temporal

### 2️⃣ Tratamento e Limpeza
- Padronização de unidades (Kg → Litros, US$ → R$)
- Tratamento de valores nulos e zeros
- Correção de categorias inconsistentes (ex.: continentes)
- Criação de variáveis derivadas (ex.: preço médio, ticket médio)

### 3️⃣ Análise Exploratória (EDA)
- Estatísticas descritivas
- Análise temporal (tendência histórica)
- Market share por país e continente
- Classificação de países por:
  - Volume
  - Valor
  - Preço médio
- Segmentação por continentes

### 4️⃣ Storytelling com Dados
- Organização lógica dos insights
- Interpretação econômica dos resultados
- Construção de uma narrativa orientada a negócio
- Conclusões baseadas em evidências

---

## 🛠️ Tecnologias Utilizadas

- **Python**
- **Pandas**
- **NumPy**
- **Matplotlib / Seaborn**
- **Statsmodels**

---

## 📂 Estrutura do Repositório

```text
├── data/                 # Bases originais e tratadas
├── notebooks/            # Notebooks de análise e exploração
├── src/                  # Scripts auxiliares
├── assets/               # Imagens e recursos visuais
├── README.md             # Documentação do projeto
└── LICENSE
```

## 💻 Como Usar

1. Clone o repositório:
```bash
git clone https://github.com/Wesley-Alexsander/FIAP_Tech_Challenge_01.git
```
2. Execute os exemplos:
```bash
cd  /FIAP_Tech_Challenge_01/Notebooks/exp_exploration.ipynb
```
3. Execute a Pipeline:
```bash
python3 Scripts/main.py
```


## 🤝 Contribuição
Encontrou um erro ou tem uma solução melhor? Sinta-se à vontade para:

- Abrir uma Issue

- Enviar um Pull Request

- Compartilhar ideias de melhorias

---
📜 Licença
Este projeto está licenciado sob a MIT License - veja o arquivo LICENSE para detalhes.
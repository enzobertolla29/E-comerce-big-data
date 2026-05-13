# Claude.md

## Visao geral
Este projeto faz processamento de logs de e-commerce com Python e pandas. O fluxo principal e:

1. Ler o CSV bruto em `E-commerce Website Logs.csv`.
2. Particionar os dados por `ano/mes/dia/hora` em `dados_particionados/`.
3. Registrar linhas inconsistentes em `controle_lote/`.
4. Gerar vistas agregadas em `vistas/` a partir das particoes.

## Arquivos principais
- `particionar.py`: executa o particionamento, aplica regras de quarentena e grava o estado do lote.
- `vista_particao.py`: consome os CSVs particionados e gera as vistas finais.

## Saidas geradas
- `dados_particionados/`: particoes no formato `ano=YYYY/mes=MM/dia=DD/hora=HH/dados.csv`.
- `controle_lote/estado_lote.json`: estado de processamento e controle de reprocessamento.
- `controle_lote/quarentena_linhas.csv`: linhas registradas em quarentena.
- `controle_lote/quarentena_contagem_por_arquivo.csv`: resumo por arquivo processado.
- `vistas/`: arquivos CSV consolidados das analises.

## Como executar
- Para particionar os dados: `python particionar.py`
- Para forcar reprocessamento: `python particionar.py --force`
- Para usar outro CSV de entrada: `python particionar.py --input caminho\\para\\arquivo.csv`
- Para gerar as vistas: `python vista_particao.py`

## Regras do processamento
- A coluna `accessed_date` e obrigatoria.
- Linhas com data invalida entram em quarentena bloqueante e nao sao escritas nas particoes.
- Regras adicionais de quarentena tratam `gender`, `membership` e `age` quando essas colunas existem.
- O script de particionamento evita reprocessar o mesmo arquivo se nada mudou, a menos que `--force` seja usado.

## Orientacoes para alteracoes
- Prefira mudancas pequenas e localizadas.
- Nao edite os CSVs gerados manualmente, a menos que o objetivo seja corrigir uma saida especifica.
- Se mudar a logica de particionamento, verifique tambem o consumo em `vista_particao.py`.
- Preserve os nomes das colunas de saida, porque as vistas dependem deles.

## Parte 3 — Camada de Velocidade

### Arquivos adicionais
- `separar_dados_novos.py`: separa os 10% de registros mais recentes do CSV principal e grava em `dados_novos/fluxo.log` (sem cabecalho). Tambem grava `dados_novos/info_corte.txt` com a data de corte e nomes das colunas.
- `camada_velocidade.py`: implementa o streaming (thread + yield conforme esboço do PDF), processa vistas de tempo real incrementalmente em memória e combina com as vistas de lote para reconstruir as consultas da Etapa 1.

### Saidas da Parte 3
- `dados_novos/fluxo.log`: linhas CSV sem cabecalho (dados recentes para streaming).
- `dados_novos/fluxo_live.log`: arquivo temporario usado pelo monitor no modo automatico.
- `dados_novos/info_corte.txt`: metadados do corte (data, colunas, contagens).
- `vistas_tempo_real/vtr1_pais_pagamento.csv`: VTR1 bruta.
- `vistas_tempo_real/vtr2_faixa_membership.csv`: VTR2 bruta.
- `vistas_tempo_real/vtr3_hora_genero.csv`: VTR3 bruta.
- `vistas_tempo_real/vtr4_correlacao.csv`: VTR4 — correlação online sales x returned.
- `vistas_tempo_real/consulta1_pais_pagamento.csv`: lote + TR combinados.
- `vistas_tempo_real/consulta2_faixa_etaria_membership.csv`: lote + TR combinados.
- `vistas_tempo_real/consulta3_hora_genero.csv`: lote + TR combinados.
- `vistas_tempo_real/consulta4_correlacao_sales_returned.csv`: correlação TR.

### Como executar a Parte 3
1. Garantir que o particionamento ja foi executado: `python particionar.py`
2. Garantir que as vistas de lote existem: `python vista_particao.py`
3. Separar os dados novos: `python separar_dados_novos.py`
4. Executar a camada de velocidade: `python camada_velocidade.py`
   - `--delay 0.05`: intervalo entre linhas no modo automatico (padrao: 0.02s)
   - `--manual`: aguarda insercao manual de linhas em fluxo.log

## Observacoes de manutencao
- O projeto usa caminhos relativos ao diretorio `trabalho_grandes_volumes`.
- O codigo foi escrito para rodar localmente com Python e pandas.
- Se adicionar novos artefatos gerados, documente o caminho e o proposito neste arquivo.
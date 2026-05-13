"""
Separa os dados mais recentes do CSV principal para simular a Camada de Velocidade.

Os dados anteriores ao corte permanecem acessíveis à Camada de Lote (já particionados).
Os dados a partir do corte são gravados em dados_novos/fluxo.log — um arquivo sem
cabeçalho onde cada linha é um registro CSV cru, simulando um fluxo de streaming.

Uso:
    python separar_dados_novos.py              # usa o CSV padrão
    python separar_dados_novos.py --input X    # CSV alternativo
    python separar_dados_novos.py --pct 0.15   # fração dos dados recentes (padrão: 0.10)
    python separar_dados_novos.py --force       # regrava mesmo se fluxo.log já existir
"""

import argparse
import os

import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

ENTRADA_PADRAO = os.path.join(BASE_DIR, "E-commerce Website Logs.csv")
DIR_DADOS_NOVOS = os.path.join(BASE_DIR, "dados_novos")
ARQ_FLUXO = os.path.join(DIR_DADOS_NOVOS, "fluxo.log")
ARQ_INFO = os.path.join(DIR_DADOS_NOVOS, "info_corte.txt")


def separar(arquivo_entrada: str = ENTRADA_PADRAO, pct: float = 0.10, force: bool = False) -> None:
    if not force and os.path.exists(ARQ_FLUXO):
        print(f"fluxo.log já existe. Use --force para recriar.")
        return

    df = pd.read_csv(arquivo_entrada)

    ts = pd.to_datetime(df["accessed_date"], errors="coerce")
    df = df.loc[ts.notna()].copy()
    df["_ts"] = pd.to_datetime(df["accessed_date"], errors="coerce")
    df = df.sort_values("_ts")

    n_novos = max(1, int(len(df) * pct))
    corte_idx = len(df) - n_novos
    data_corte = df.iloc[corte_idx]["_ts"]

    df_novos = df.iloc[corte_idx:].drop(columns=["_ts"])

    os.makedirs(DIR_DADOS_NOVOS, exist_ok=True)

    # fluxo.log: sem cabeçalho, uma linha por registro (simula chegada incremental)
    df_novos.to_csv(ARQ_FLUXO, index=False, header=False)

    with open(ARQ_INFO, "w", encoding="utf-8") as f:
        f.write(f"data_corte={data_corte.isoformat()}\n")
        f.write(f"registros_novos={len(df_novos)}\n")
        f.write(f"registros_lote={corte_idx}\n")
        f.write(f"colunas={','.join(df_novos.columns.tolist())}\n")

    print(
        f"Separação concluída:\n"
        f"  Corte em: {data_corte}\n"
        f"  Registros na camada de lote : {corte_idx:,}\n"
        f"  Registros no fluxo (novos)  : {len(df_novos):,}\n"
        f"  Arquivo gerado: {ARQ_FLUXO}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Separa dados recentes para simular streaming.")
    parser.add_argument("--input", default=ENTRADA_PADRAO)
    parser.add_argument("--pct", type=float, default=0.10,
                        help="Fração dos dados mais recentes a separar (padrão: 0.10 = 10%%)")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    separar(arquivo_entrada=args.input, pct=args.pct, force=args.force)

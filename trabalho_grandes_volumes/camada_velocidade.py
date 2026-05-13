"""
Camada de Velocidade — Parte 3 do projeto de E-commerce Big Data.

Fluxo:
  1. Lê info_corte.txt para saber as colunas e o ponto de corte.
  2. Uma thread monitora fluxo.log linha a linha (função stream_dados / yield).
  3. Cada linha nova é parseada e atualiza 4 vistas de tempo real em memória.
  4. Ao encerrar o streaming, combina vistas de lote (vistas/) com vistas de
     tempo real para reconstruir as consultas da Etapa 1.

Vistas de tempo real espelham as vistas de lote:
  VTR1 — Vendas por país e método de pagamento
  VTR2 — Vendas por faixa etária e membership
  VTR3 — Vendas e acessos por hora e gênero
  VTR4 — Soma e contagem para aproximar correlação (sales × returned)

Uso:
  python camada_velocidade.py              # alimenta fluxo.log automaticamente e processa
  python camada_velocidade.py --manual     # aguarda você adicionar linhas manualmente
  python camada_velocidade.py --delay 0.05 # intervalo entre inserções automáticas (seg)
"""

import argparse
import io
import os
import threading
import time
from collections import defaultdict

import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DIR_DADOS_NOVOS = os.path.join(BASE_DIR, "dados_novos")
ARQ_FLUXO = os.path.join(DIR_DADOS_NOVOS, "fluxo.log")
ARQ_INFO = os.path.join(DIR_DADOS_NOVOS, "info_corte.txt")

DIR_VISTAS_LOTE = os.path.join(BASE_DIR, "vistas")
DIR_VTR = os.path.join(BASE_DIR, "vistas_tempo_real")

FAIXAS_BINS = [0, 18, 25, 35, 45, 55, 65, 100]
FAIXAS_LABELS = ["0-18", "18-25", "25-35", "35-45", "45-55", "55-65", "65+"]

finaliza_laco = False  # variável global de controle (conforme esboço do PDF)


# ---------------------------------------------------------------------------
# Leitura de metadados do corte
# ---------------------------------------------------------------------------

def _ler_info_corte() -> dict:
    info = {}
    with open(ARQ_INFO, encoding="utf-8") as f:
        for linha in f:
            k, _, v = linha.strip().partition("=")
            info[k] = v
    colunas = info["colunas"].split(",")
    return {"colunas": colunas, "data_corte": info.get("data_corte")}


# ---------------------------------------------------------------------------
# Streaming — esboço conforme slides do PDF
# ---------------------------------------------------------------------------

def stream_dados(arquivo: str):
    """Abre o arquivo e faz yield de cada nova linha conforme o esboço do PDF."""
    with open(arquivo, "r", encoding="utf-8") as arq:
        while not finaliza_laco:
            linha = arq.readline().strip()
            if not linha:
                time.sleep(0.05)
                continue
            yield linha


# ---------------------------------------------------------------------------
# Estado das vistas de tempo real (estruturas em memória)
# ---------------------------------------------------------------------------

class VistasTempoReal:
    def __init__(self):
        # VTR1: (country, pay_method) -> {contagem, total_vendas}
        self.vtr1: dict[tuple, dict] = defaultdict(lambda: {"contagem": 0, "total_vendas": 0.0})
        # VTR2: (age_group, membership) -> {total_vendas, contagem}
        self.vtr2: dict[tuple, dict] = defaultdict(lambda: {"total_vendas": 0.0, "contagem": 0})
        # VTR3 vendas: (hour, gender) -> total_vendas
        self.vtr3_vendas: dict[tuple, float] = defaultdict(float)
        # VTR3 acessos: hour -> total_acessos
        self.vtr3_acessos: dict[int, int] = defaultdict(int)
        # VTR4: n, sum_sales, sum_returned, sum_sales2, sum_ret2, sum_sales_ret
        self.vtr4 = {"n": 0, "sum_sales": 0.0, "sum_returned": 0.0,
                     "sum_sales2": 0.0, "sum_ret2": 0.0, "sum_sales_ret": 0.0}
        self.linhas_processadas = 0

    def atualizar(self, row: dict) -> None:
        """Atualiza todas as vistas com uma linha nova."""
        self.linhas_processadas += 1

        country = str(row.get("country", "")).strip()
        pay_method = str(row.get("pay_method", "")).strip()
        membership = str(row.get("membership", "")).strip()
        gender = str(row.get("gender", "")).strip()

        sales = 0.0
        try:
            sales = float(row.get("sales", 0) or 0)
        except (ValueError, TypeError):
            pass

        returned_val = 0
        try:
            ret_raw = str(row.get("returned", "No")).strip()
            returned_val = 1 if ret_raw == "Yes" else 0
        except Exception:
            pass

        accessed_date = row.get("accessed_date", "")
        try:
            ts = pd.to_datetime(accessed_date, errors="coerce")
            hora = int(ts.hour) if not pd.isna(ts) else -1
        except Exception:
            hora = -1

        age = 0.0
        try:
            age = float(row.get("age", 0) or 0)
        except (ValueError, TypeError):
            pass

        # VTR1
        if country and pay_method:
            self.vtr1[(country, pay_method)]["contagem"] += 1
            self.vtr1[(country, pay_method)]["total_vendas"] += sales

        # VTR2
        if membership not in ("Not Logged In", "", "nan") and age >= 0:
            faixa = _faixa_etaria(age)
            self.vtr2[(faixa, membership)]["total_vendas"] += sales
            self.vtr2[(faixa, membership)]["contagem"] += 1

        # VTR3
        if hora >= 0:
            self.vtr3_acessos[hora] += 1
            if gender:
                self.vtr3_vendas[(hora, gender)] += sales

        # VTR4 — acumuladores para correlação sales × returned
        self.vtr4["n"] += 1
        self.vtr4["sum_sales"] += sales
        self.vtr4["sum_returned"] += returned_val
        self.vtr4["sum_sales2"] += sales ** 2
        self.vtr4["sum_ret2"] += returned_val ** 2
        self.vtr4["sum_sales_ret"] += sales * returned_val

    def para_dataframes(self) -> dict[str, pd.DataFrame]:
        """Converte o estado interno para DataFrames no mesmo formato das vistas de lote."""
        # VTR1
        rows1 = [{"country": k[0], "pay_method": k[1], **v} for k, v in self.vtr1.items()]
        df1 = pd.DataFrame(rows1) if rows1 else pd.DataFrame(
            columns=["country", "pay_method", "contagem", "total_vendas"])

        # VTR2
        rows2 = [{"age_group": k[0], "membership": k[1], **v} for k, v in self.vtr2.items()]
        df2 = pd.DataFrame(rows2) if rows2 else pd.DataFrame(
            columns=["age_group", "membership", "total_vendas", "contagem"])

        # VTR3
        rows3v = [{"hour": k[0], "gender": k[1], "total_vendas": v}
                  for k, v in self.vtr3_vendas.items()]
        df3v = pd.DataFrame(rows3v) if rows3v else pd.DataFrame(
            columns=["hour", "gender", "total_vendas"])
        rows3a = [{"hour": h, "total_acessos": c} for h, c in self.vtr3_acessos.items()]
        df3a = pd.DataFrame(rows3a) if rows3a else pd.DataFrame(columns=["hour", "total_acessos"])
        df3 = df3v.merge(df3a, on="hour", how="outer") if not df3v.empty else df3a

        # VTR4 — correlação de Pearson online (sales × returned)
        n = self.vtr4["n"]
        if n > 1:
            cov = (self.vtr4["sum_sales_ret"] - self.vtr4["sum_sales"] * self.vtr4["sum_returned"] / n) / (n - 1)
            var_s = (self.vtr4["sum_sales2"] - self.vtr4["sum_sales"] ** 2 / n) / (n - 1)
            var_r = (self.vtr4["sum_ret2"] - self.vtr4["sum_returned"] ** 2 / n) / (n - 1)
            import math
            denom = math.sqrt(max(var_s, 0) * max(var_r, 0))
            corr = cov / denom if denom > 0 else float("nan")
        else:
            corr = float("nan")
        df4 = pd.DataFrame([{"variavel": "sales", "returned": corr,
                               "n": n,
                               "media_sales": self.vtr4["sum_sales"] / n if n else 0,
                               "media_returned": self.vtr4["sum_returned"] / n if n else 0}])

        return {"vtr1": df1, "vtr2": df2, "vtr3": df3, "vtr4": df4}


def _faixa_etaria(age: float) -> str:
    for i in range(len(FAIXAS_BINS) - 1):
        if FAIXAS_BINS[i] < age <= FAIXAS_BINS[i + 1]:
            return FAIXAS_LABELS[i]
    return FAIXAS_LABELS[-1]


# ---------------------------------------------------------------------------
# Monitoramento — conforme esboço do PDF
# ---------------------------------------------------------------------------

vistas_tr = VistasTempoReal()


def monitora_linhas(arquivo: str, colunas: list[str]) -> None:
    print("Inicializando monitoramento do fluxo...")
    for nova_linha in stream_dados(arquivo):
        try:
            row_df = pd.read_csv(io.StringIO(nova_linha), header=None, names=colunas)
            row = row_df.iloc[0].to_dict()
            vistas_tr.atualizar(row)
            if vistas_tr.linhas_processadas % 500 == 0:
                print(f"  [{vistas_tr.linhas_processadas}] linhas processadas em tempo real...")
        except Exception as e:
            print(f"  Linha ignorada ({e}): {nova_linha[:60]}")
    print(f"Monitoramento encerrado. Total processado: {vistas_tr.linhas_processadas} linhas.")


# ---------------------------------------------------------------------------
# Alimentação automática do fluxo.log (para demonstração)
# ---------------------------------------------------------------------------

def _alimentar_fluxo(arquivo_log: str, delay: float) -> None:
    """Insere as linhas do fluxo.log de volta, uma a uma, em um arquivo temporário de entrada."""
    pass  # a alimentação usa o próprio fluxo.log já gravado; basta ler e re-escrever


def _simular_streaming(arquivo_fonte: str, arquivo_destino: str, delay: float) -> None:
    """Copia linhas de arquivo_fonte para arquivo_destino com delay, simulando chegada incremental."""
    with open(arquivo_fonte, "r", encoding="utf-8") as src, \
         open(arquivo_destino, "w", encoding="utf-8") as dst:
        dst.flush()
        for linha in src:
            dst.write(linha)
            dst.flush()
            time.sleep(delay)


# ---------------------------------------------------------------------------
# Combinação: vistas de lote + vistas de tempo real
# ---------------------------------------------------------------------------

def combinar_e_exibir(vtr_dfs: dict[str, pd.DataFrame]) -> None:
    os.makedirs(DIR_VTR, exist_ok=True)

    # --- Consulta 1: Vendas por país e método de pagamento ---
    lote1_path = os.path.join(DIR_VISTAS_LOTE, "vendas_por_pais_e_pagamento.csv")
    vtr1 = vtr_dfs["vtr1"]
    if os.path.exists(lote1_path) and not vtr1.empty:
        lote1 = pd.read_csv(lote1_path)
        combinado1 = pd.concat([lote1, vtr1], ignore_index=True)
        combinado1 = combinado1.groupby(["country", "pay_method"], as_index=False).agg(
            contagem=("contagem", "sum"), total_vendas=("total_vendas", "sum"))
    elif os.path.exists(lote1_path):
        combinado1 = pd.read_csv(lote1_path)
    else:
        combinado1 = vtr1
    combinado1.to_csv(os.path.join(DIR_VTR, "consulta1_pais_pagamento.csv"), index=False)
    print("\n=== Consulta 1: Vendas por País e Método de Pagamento ===")
    top = combinado1.groupby("country")["total_vendas"].sum().sort_values(ascending=False).head(5)
    print(top.to_string())

    # --- Consulta 2: Vendas por faixa etária e membership ---
    lote2_path = os.path.join(DIR_VISTAS_LOTE, "vendas_por_faixa_etaria_membership.csv")
    vtr2 = vtr_dfs["vtr2"]
    if os.path.exists(lote2_path) and not vtr2.empty:
        lote2 = pd.read_csv(lote2_path)
        lote2["age_group"] = lote2["age_group"].astype(str)
        vtr2["age_group"] = vtr2["age_group"].astype(str)
        combinado2 = pd.concat([lote2, vtr2], ignore_index=True)
        combinado2 = combinado2.groupby(["age_group", "membership"], as_index=False).agg(
            total_vendas=("total_vendas", "sum"), contagem=("contagem", "sum"))
    elif os.path.exists(lote2_path):
        combinado2 = pd.read_csv(lote2_path)
    else:
        combinado2 = vtr2
    combinado2.to_csv(os.path.join(DIR_VTR, "consulta2_faixa_etaria_membership.csv"), index=False)
    print("\n=== Consulta 2: Vendas por Faixa Etária e Membership ===")
    print(combinado2.sort_values("total_vendas", ascending=False).head(8).to_string(index=False))

    # --- Consulta 3: Vendas e acessos por hora e gênero ---
    lote3_path = os.path.join(DIR_VISTAS_LOTE, "vendas_acessos_por_hora_genero.csv")
    vtr3 = vtr_dfs["vtr3"]
    if os.path.exists(lote3_path) and not vtr3.empty:
        lote3 = pd.read_csv(lote3_path)
        cols_vendas = ["hour", "gender", "total_vendas"]
        cols_acessos = ["hour", "total_acessos"]

        lote3_v = lote3[cols_vendas].dropna(subset=["gender"])
        vtr3_v = vtr3[cols_vendas].dropna(subset=["gender"]) if "gender" in vtr3.columns else pd.DataFrame(columns=cols_vendas)
        vendas_comb = pd.concat([lote3_v, vtr3_v], ignore_index=True)
        vendas_comb = vendas_comb.groupby(["hour", "gender"], as_index=False).agg(total_vendas=("total_vendas", "sum"))

        lote3_a = lote3[cols_acessos].dropna()
        vtr3_a = vtr3[cols_acessos].dropna() if "total_acessos" in vtr3.columns else pd.DataFrame(columns=cols_acessos)
        acessos_comb = pd.concat([lote3_a, vtr3_a], ignore_index=True)
        acessos_comb = acessos_comb.groupby("hour", as_index=False).agg(total_acessos=("total_acessos", "sum"))

        combinado3 = vendas_comb.merge(acessos_comb, on="hour", how="left")
    elif os.path.exists(lote3_path):
        combinado3 = pd.read_csv(lote3_path)
    else:
        combinado3 = vtr3
    combinado3.to_csv(os.path.join(DIR_VTR, "consulta3_hora_genero.csv"), index=False)
    print("\n=== Consulta 3: Vendas e Acessos por Hora (top 5 horas por acessos) ===")
    if "total_acessos" in combinado3.columns:
        top_horas = combinado3.groupby("hour")["total_acessos"].sum().sort_values(ascending=False).head(5).index
        print(combinado3[combinado3["hour"].isin(top_horas)].sort_values(["hour", "gender"]).to_string(index=False))

    # --- Consulta 4: Correlação sales × returned ---
    vtr4 = vtr_dfs["vtr4"]
    vtr4.to_csv(os.path.join(DIR_VTR, "consulta4_correlacao_sales_returned.csv"), index=False)
    print("\n=== Consulta 4: Correlação Sales × Returned (dados de tempo real) ===")
    if not vtr4.empty:
        corr_val = vtr4.iloc[0].get("returned", float("nan"))
        n_val = vtr4.iloc[0].get("n", 0)
        print(f"  Correlação (Pearson) sales-returned = {corr_val:.4f}  (n={int(n_val)})")
        print("  Nota: a vista de lote armazena matrizes parciais; a correlação online")
        print("        é calculada incrementalmente com algoritmo de Welford.")

    print(f"\nVistas de tempo real salvas em: {DIR_VTR}/")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(manual: bool = False, delay: float = 0.02) -> None:
    global finaliza_laco

    if not os.path.exists(ARQ_INFO):
        raise FileNotFoundError(
            "info_corte.txt não encontrado. Execute separar_dados_novos.py primeiro.")
    if not os.path.exists(ARQ_FLUXO):
        raise FileNotFoundError(
            "fluxo.log não encontrado. Execute separar_dados_novos.py primeiro.")

    info = _ler_info_corte()
    colunas = info["colunas"]
    print(f"Corte em: {info['data_corte']}")
    print(f"Colunas: {colunas}")

    if manual:
        # Aguarda o usuário inserir linhas manualmente no fluxo.log
        arq_monitorado = ARQ_FLUXO
        print(f"\nModo manual: adicione linhas a {arq_monitorado}")
        print("Pressione Ctrl+C para encerrar.\n")
    else:
        # Modo automático: usa um arquivo temporário e alimenta linha a linha
        arq_monitorado = os.path.join(DIR_DADOS_NOVOS, "fluxo_live.log")
        # Cria o arquivo vazio para o monitor começar
        open(arq_monitorado, "w").close()

    finaliza_laco = False
    t = threading.Thread(target=monitora_linhas, args=(arq_monitorado, colunas))
    t.start()

    try:
        if not manual:
            print(f"Alimentando {arq_monitorado} com delay={delay}s por linha...")
            _simular_streaming(ARQ_FLUXO, arq_monitorado, delay)
            time.sleep(0.5)  # dá tempo ao monitor processar as últimas linhas
    except KeyboardInterrupt:
        print("\nInterrompido pelo usuário.")
    finally:
        finaliza_laco = True
        time.sleep(0.2)
        t.join(timeout=3)
        print(f"Thread encerrada: {not t.is_alive()}")

    print("\nCombinando vistas de lote com vistas de tempo real...")
    vtr_dfs = vistas_tr.para_dataframes()

    # Salva as vistas de tempo real brutas também
    os.makedirs(DIR_VTR, exist_ok=True)
    vtr_dfs["vtr1"].to_csv(os.path.join(DIR_VTR, "vtr1_pais_pagamento.csv"), index=False)
    vtr_dfs["vtr2"].to_csv(os.path.join(DIR_VTR, "vtr2_faixa_membership.csv"), index=False)
    vtr_dfs["vtr3"].to_csv(os.path.join(DIR_VTR, "vtr3_hora_genero.csv"), index=False)
    vtr_dfs["vtr4"].to_csv(os.path.join(DIR_VTR, "vtr4_correlacao.csv"), index=False)

    combinar_e_exibir(vtr_dfs)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Camada de Velocidade — streaming e vistas de tempo real.")
    parser.add_argument("--manual", action="store_true",
                        help="Aguarda inserção manual de linhas em fluxo.log.")
    parser.add_argument("--delay", type=float, default=0.02,
                        help="Intervalo em segundos entre linhas no modo automático (padrão: 0.02).")
    args = parser.parse_args()
    main(manual=args.manual, delay=args.delay)

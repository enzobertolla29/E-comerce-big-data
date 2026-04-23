import json
import os
import uuid
import argparse
from datetime import datetime
from functools import reduce

import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

ENTRADA_PADRAO   = os.path.join(BASE_DIR, "E-commerce Website Logs.csv")
DIR_PARTICOES    = os.path.join(BASE_DIR, "dados_particionados")
DIR_CONTROLE     = os.path.join(BASE_DIR, "controle_lote")

ARQ_ESTADO             = os.path.join(DIR_CONTROLE, "estado_lote.json")
ARQ_QUARENTENA_LINHAS  = os.path.join(DIR_CONTROLE, "quarentena_linhas.csv")
ARQ_QUARENTENA_CONTAGEM= os.path.join(DIR_CONTROLE, "quarentena_contagem_por_arquivo.csv")

CUTOFF_IDADE          = pd.Timestamp("2017-03-19 02:58:00")
MEMBERSHIP_NAO_LOGADO = "Not Logged In"
VERSAO_PROCESSAMENTO  = 2


# ---------------------------------------------------------------------------
# Helpers de I/O
# ---------------------------------------------------------------------------

def _carregar_estado() -> dict:
    if not os.path.exists(ARQ_ESTADO):
        return {"schema_version": 1, "files": {}}
    with open(ARQ_ESTADO, encoding="utf-8") as f:
        return json.load(f)


def _salvar_estado(estado: dict) -> None:
    os.makedirs(DIR_CONTROLE, exist_ok=True)
    tmp = f"{ARQ_ESTADO}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(estado, f, ensure_ascii=False, indent=2)
    os.replace(tmp, ARQ_ESTADO)


def _fingerprint_arquivo(caminho: str) -> dict:
    st = os.stat(caminho)
    return {"size": st.st_size, "mtime": int(st.st_mtime)}


def _append_csv(df: pd.DataFrame, caminho: str) -> None:
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    df.to_csv(caminho, mode="a", index=False, header=not os.path.exists(caminho))


# ---------------------------------------------------------------------------
# Quarentena
# ---------------------------------------------------------------------------

def _registrar_quarentena(
    df: pd.DataFrame,
    mask: pd.Series,
    *,
    source_file: str,
    reason: str,
    is_blocking: bool,
) -> int:
    if not mask.any():                          
        return 0
    q = df.loc[mask].copy()
    q.insert(0, "source_file", source_file)
    q.insert(1, "reason", reason)
    q.insert(2, "is_blocking", is_blocking)
    q.insert(3, "source_row_number", q.index + 2)
    _append_csv(q, ARQ_QUARENTENA_LINHAS)
    return int(mask.sum())


def _construir_mascaras(df: pd.DataFrame, ts: pd.Series) -> list[dict]:
    """
    Retorna uma lista de dicts com as regras de quarentena.
    Centralizar aqui facilita adicionar/remover regras no futuro.
    """
    rules = []

    # 1. Data inválida — bloqueante
    rules.append({
        "key": "accessed_date_invalid",
        "mask": ts.isna(),
        "is_blocking": True,
    })

    # 2. Gênero inconsistente — não bloqueante
    if {"gender", "membership"}.issubset(df.columns):
        mask = (
            df["gender"].astype(str).str.strip().str.casefold().eq("unknown")
            & df["membership"].astype(str).str.strip().ne(MEMBERSHIP_NAO_LOGADO)
        )
    else:
        mask = pd.Series(False, index=df.index)
    rules.append({"key": "gender_unknown_logged_in", "mask": mask, "is_blocking": False})

    # 3. Idade ausente pós-corte — não bloqueante
    if {"age", "membership"}.issubset(df.columns):
        age_num = pd.to_numeric(df["age"], errors="coerce")
        mask = (
            ts.ge(CUTOFF_IDADE)
            & df["membership"].astype(str).str.strip().ne(MEMBERSHIP_NAO_LOGADO)
            & (age_num.isna() | age_num.eq(0))
        )
    else:
        mask = pd.Series(False, index=df.index)
    rules.append({"key": "age_missing_after_cutoff", "mask": mask, "is_blocking": False})

    return rules


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def particionar(arquivo_entrada: str = ENTRADA_PADRAO, *, force: bool = False) -> None:
    if not os.path.exists(arquivo_entrada):
        raise FileNotFoundError(f"Arquivo de entrada não encontrado: {arquivo_entrada}")

    os.makedirs(DIR_PARTICOES, exist_ok=True)
    os.makedirs(DIR_CONTROLE, exist_ok=True)

    estado = _carregar_estado()
    chave  = os.path.basename(arquivo_entrada)
    fp     = _fingerprint_arquivo(arquivo_entrada)
    anterior = estado.get("files", {}).get(chave)

    if (
        not force
        and anterior
        and anterior.get("last_status") == "completed"
        and anterior.get("source") == fp
        and anterior.get("processing_version") == VERSAO_PROCESSAMENTO
    ):
        print(f"Nada a fazer (já processado): {chave}")
        return

    run_id     = uuid.uuid4().hex
    started_at = datetime.now().isoformat(timespec="seconds")

    estado.setdefault("files", {}).setdefault(chave, {}).update({
        "last_status":        "running",
        "last_run_id":        run_id,
        "last_started_at":    started_at,
        "source":             fp,
        "processing_version": VERSAO_PROCESSAMENTO,
        "output_dir":         "dados_particionados",
    })
    _salvar_estado(estado)

    try:
        df = pd.read_csv(arquivo_entrada)

        if "accessed_date" not in df.columns:
            raise ValueError("Coluna obrigatória ausente: accessed_date")

        ts    = pd.to_datetime(df["accessed_date"], errors="coerce")
        rules = _construir_mascaras(df, ts)

        # Registra quarentena e coleta contagens
        contagens: dict[str, int] = {}
        for rule in rules:
            contagens[rule["key"]] = _registrar_quarentena(
                df,
                rule["mask"],
                source_file=chave,
                reason=rule["key"],
                is_blocking=rule["is_blocking"],
            )

        # Máscara total e bloqueante
        mask_total    = reduce(lambda a, b: a | b, (r["mask"] for r in rules))
        mask_blocking = reduce(lambda a, b: a | b, (r["mask"] for r in rules if r["is_blocking"]))

        qtd_total    = int(mask_total.sum())
        qtd_blocking = int(mask_blocking.sum())

        # Somente linhas sem data inválida vão para as partições
        df_ok = df.loc[~mask_blocking].copy()
        df_ok["timestamp"] = ts.loc[~mask_blocking]
        df_ok["ano"]  = df_ok["timestamp"].dt.year
        df_ok["mes"]  = df_ok["timestamp"].dt.month
        df_ok["dia"]  = df_ok["timestamp"].dt.day
        df_ok["hora"] = df_ok["timestamp"].dt.hour

        partitions_written = rows_written = 0
        for (ano, mes, dia, hora), grupo in df_ok.groupby(
            ["ano", "mes", "dia", "hora"], dropna=False
        ):
            caminho = os.path.join(
                DIR_PARTICOES,
                f"ano={int(ano)}", f"mes={int(mes):02d}",
                f"dia={int(dia):02d}", f"hora={int(hora):02d}",
            )
            os.makedirs(caminho, exist_ok=True)
            grupo.to_csv(os.path.join(caminho, "dados.csv"), index=False)
            partitions_written += 1
            rows_written += len(grupo)

        # Log de quarentena por arquivo
        _append_csv(
            pd.DataFrame([{
                "run_id":    run_id,
                "timestamp": started_at,
                "source_file": chave,
                "added_to_quarantine_total":        qtd_total,
                "added_to_quarantine_blocking":     qtd_blocking,
                **{f"added_to_quarantine_{k}": v for k, v in contagens.items()},
            }]),
            ARQ_QUARENTENA_CONTAGEM,
        )

        estado["files"][chave].update({
            "last_status":       "completed",
            "last_completed_at": datetime.now().isoformat(timespec="seconds"),
            "rows_total":                          int(len(df)),
            "rows_quarantined_total":              qtd_total,
            "rows_quarantined_blocking":           qtd_blocking,
            **{f"rows_quarantined_{k}": v for k, v in contagens.items()},
            "rows_written":      int(rows_written),
            "partitions_written":int(partitions_written),
            "processing_version":VERSAO_PROCESSAMENTO,
        })
        _salvar_estado(estado)

        print(
            f"Processado: {chave} | total={len(df)} | quarentena_total={qtd_total} "
            f"(bloqueante={qtd_blocking}, "
            + ", ".join(f"{k}={v}" for k, v in contagens.items())
            + f") | gravadas={rows_written} | partições={partitions_written}"
        )

    except Exception as e:
        estado["files"][chave].update({
            "last_status":   "failed",
            "last_failed_at": datetime.now().isoformat(timespec="seconds"),
            "error":         str(e),
        })
        _salvar_estado(estado)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Particiona logs de e-commerce e aplica regras de quarentena."
    )
    parser.add_argument("--force", action="store_true",
                        help="Força reprocessamento mesmo se já concluído.")
    parser.add_argument("--input", default=ENTRADA_PADRAO,
                        help="Caminho para o CSV de entrada.")
    args = parser.parse_args()
    particionar(arquivo_entrada=args.input, force=args.force)
import json
import os
import uuid
import argparse
from datetime import datetime

import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

ENTRADA_PADRAO = os.path.join(BASE_DIR, "E-commerce Website Logs.csv")
DIR_PARTICOES = os.path.join(BASE_DIR, "dados_particionados")
DIR_CONTROLE = os.path.join(BASE_DIR, "controle_lote")

ARQ_ESTADO = os.path.join(DIR_CONTROLE, "estado_lote.json")
ARQ_QUARENTENA_LINHAS = os.path.join(DIR_CONTROLE, "quarentena_linhas.csv")
ARQ_QUARENTENA_CONTAGEM = os.path.join(DIR_CONTROLE, "quarentena_contagem_por_arquivo.csv")

CUTOFF_IDADE = pd.Timestamp("2017-03-19 02:58:00")
MEMBERSHIP_NAO_LOGADO = "Not Logged In"
VERSAO_PROCESSAMENTO = 2


def _carregar_estado() -> dict:
    if not os.path.exists(ARQ_ESTADO):
        return {"schema_version": 1, "files": {}}
    with open(ARQ_ESTADO, "r", encoding="utf-8") as f:
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
    header = not os.path.exists(caminho)
    df.to_csv(caminho, mode="a", index=False, header=header)


def _registrar_quarentena(df: pd.DataFrame, mask: pd.Series, *, source_file: str, reason: str, is_blocking: bool) -> int:
    if not bool(mask.any()):
        return 0
    df_quarentena = df.loc[mask].copy()
    df_quarentena.insert(0, "source_file", source_file)
    df_quarentena.insert(1, "reason", reason)
    df_quarentena.insert(2, "is_blocking", is_blocking)
    df_quarentena.insert(3, "source_row_number", df_quarentena.index + 2)  # +1 header, +1 base 1
    _append_csv(df_quarentena, ARQ_QUARENTENA_LINHAS)
    return int(mask.sum())


def particionar(arquivo_entrada: str = ENTRADA_PADRAO, *, force: bool = False) -> None:
    os.makedirs(DIR_PARTICOES, exist_ok=True)
    os.makedirs(DIR_CONTROLE, exist_ok=True)

    estado = _carregar_estado()
    chave = os.path.basename(arquivo_entrada)

    if not os.path.exists(arquivo_entrada):
        raise FileNotFoundError(f"Arquivo de entrada não encontrado: {arquivo_entrada}")

    fp = _fingerprint_arquivo(arquivo_entrada)
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

    run_id = uuid.uuid4().hex
    started_at = datetime.now().isoformat(timespec="seconds")

    estado.setdefault("files", {}).setdefault(chave, {})
    estado["files"][chave].update(
        {
            "last_status": "running",
            "last_run_id": run_id,
            "last_started_at": started_at,
            "source": fp,
            "processing_version": VERSAO_PROCESSAMENTO,
            "output_dir": os.path.relpath(DIR_PARTICOES, BASE_DIR).replace("\\", "/"),
        }
    )
    _salvar_estado(estado)

    try:
        df = pd.read_csv(arquivo_entrada)

        if "accessed_date" not in df.columns:
            raise ValueError("Coluna obrigatória ausente: accessed_date")

        # Regras de quarentena
        ts = pd.to_datetime(df["accessed_date"], errors="coerce")
        mask_data_invalida = ts.isna()

        mask_genero_inconsistente = pd.Series(False, index=df.index)
        if {"gender", "membership"}.issubset(df.columns):
            mask_genero_inconsistente = (
                df["gender"].astype(str).str.strip().str.casefold().eq("unknown")
                & df["membership"].astype(str).str.strip().ne(MEMBERSHIP_NAO_LOGADO)
            )

        mask_idade_ausente_pos_corte = pd.Series(False, index=df.index)
        if {"age", "membership"}.issubset(df.columns):
            age_numerica = pd.to_numeric(df["age"], errors="coerce")
            mask_idade_ausente_pos_corte = (
                ts.ge(CUTOFF_IDADE)
                & df["membership"].astype(str).str.strip().ne(MEMBERSHIP_NAO_LOGADO)
                & (age_numerica.isna() | age_numerica.eq(0))
            )

        qtd_data_invalida = _registrar_quarentena(
            df,
            mask_data_invalida,
            source_file=chave,
            reason="accessed_date_invalid",
            is_blocking=True,
        )
        qtd_genero_inconsistente = _registrar_quarentena(
            df,
            mask_genero_inconsistente,
            source_file=chave,
            reason="gender_unknown_logged_in",
            is_blocking=False,
        )
        qtd_idade_ausente = _registrar_quarentena(
            df,
            mask_idade_ausente_pos_corte,
            source_file=chave,
            reason="age_missing_after_cutoff",
            is_blocking=False,
        )

        mask_quarentena_total = mask_data_invalida | mask_genero_inconsistente | mask_idade_ausente_pos_corte
        qtd_quarentena_total = int(mask_quarentena_total.sum())

        # Só data inválida bloqueia a partição.
        df_ok = df.loc[~mask_data_invalida].copy()
        df_ok["timestamp"] = ts.loc[~mask_data_invalida]

        # Partições de tempo
        df_ok["ano"] = df_ok["timestamp"].dt.year
        df_ok["mes"] = df_ok["timestamp"].dt.month
        df_ok["dia"] = df_ok["timestamp"].dt.day
        df_ok["hora"] = df_ok["timestamp"].dt.hour

        partitions_written = 0
        rows_written = 0

        for (ano, mes, dia, hora), grupo in df_ok.groupby(["ano", "mes", "dia", "hora"], dropna=False):
            caminho = os.path.join(
                DIR_PARTICOES,
                f"ano={int(ano)}",
                f"mes={int(mes):02d}",
                f"dia={int(dia):02d}",
                f"hora={int(hora):02d}",
            )
            os.makedirs(caminho, exist_ok=True)
            grupo.to_csv(os.path.join(caminho, "dados.csv"), index=False)
            partitions_written += 1
            rows_written += len(grupo)

        # Debug 2: contagem por arquivo (incremento desta execução)
        df_cont = pd.DataFrame(
            [
                {
                    "run_id": run_id,
                    "timestamp": started_at,
                    "source_file": chave,
                    "added_to_quarantine_total": qtd_quarentena_total,
                    "added_to_quarantine_blocking": qtd_data_invalida,
                    "added_to_quarantine_accessed_date_invalid": qtd_data_invalida,
                    "added_to_quarantine_gender_unknown_logged_in": qtd_genero_inconsistente,
                    "added_to_quarantine_age_missing_after_cutoff": qtd_idade_ausente,
                }
            ]
        )
        _append_csv(df_cont, ARQ_QUARENTENA_CONTAGEM)

        estado["files"][chave].update(
            {
                "last_status": "completed",
                "last_completed_at": datetime.now().isoformat(timespec="seconds"),
                "rows_total": int(len(df)),
                "rows_quarantined_total": qtd_quarentena_total,
                "rows_quarantined_blocking": qtd_data_invalida,
                "rows_quarantined_accessed_date_invalid": qtd_data_invalida,
                "rows_quarantined_gender_unknown_logged_in": qtd_genero_inconsistente,
                "rows_quarantined_age_missing_after_cutoff": qtd_idade_ausente,
                "rows_written": int(rows_written),
                "partitions_written": int(partitions_written),
                "processing_version": VERSAO_PROCESSAMENTO,
            }
        )
        _salvar_estado(estado)

        print(
            f"Processado: {chave} | total={len(df)} | quarentena_total={qtd_quarentena_total} "
            f"(bloqueante={qtd_data_invalida}, genero={qtd_genero_inconsistente}, idade={qtd_idade_ausente}) | "
            f"gravadas={rows_written} | partições={partitions_written}"
        )

    except Exception as e:
        estado["files"][chave].update(
            {
                "last_status": "failed",
                "last_failed_at": datetime.now().isoformat(timespec="seconds"),
                "error": str(e),
            }
        )
        _salvar_estado(estado)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Particiona logs de e-commerce e aplica regras de quarentena.")
    parser.add_argument("--force", action="store_true", help="Força reprocessamento mesmo se já estiver marcado como concluído.")
    parser.add_argument(
        "--input",
        default=ENTRADA_PADRAO,
        help="Caminho para o CSV de entrada. Padrão: E-commerce Website Logs.csv",
    )
    args = parser.parse_args()
    particionar(arquivo_entrada=args.input, force=args.force)

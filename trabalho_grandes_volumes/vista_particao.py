import pandas as pd
import os
import glob

os.makedirs("vistas", exist_ok=True)

# Listas para acumular os resultados de cada partição
resultados_vista1 = []
resultados_vista2 = []
resultados_vista3_vendas = []
resultados_vista3_acessos = []
resultados_vista4 = []

for arquivo in glob.glob("dados_particionados/**/*.csv", recursive=True):
    df = pd.read_csv(arquivo)
    df["accessed_date"] = pd.to_datetime(df["accessed_date"])
    df["hour"] = df["accessed_date"].dt.hour

    # Extrai o período do caminho da partição
    partes = arquivo.replace("\\", "/").split("/")
    ano  = partes[1].split("=")[1]
    mes  = partes[2].split("=")[1]
    dia  = partes[3].split("=")[1]
    hora = partes[4].split("=")[1]

    def add_particao(df_resultado):
        df_resultado["ano"]  = ano
        df_resultado["mes"]  = mes
        df_resultado["dia"]  = dia
        df_resultado["hora"] = hora
        return df_resultado

    # VISTA 1 — Vendas por país e método de pagamento

    v1 = (
        df.groupby(["country", "pay_method"])
        .agg(contagem=("sales", "count"), total_vendas=("sales", "sum"))
        .reset_index()
    )
    resultados_vista1.append(add_particao(v1))

    # VISTA 2 — Vendas por faixa etária e membership

    df["age_group"] = pd.cut(
        pd.to_numeric(df["age"], errors="coerce"),
        bins=[0, 18, 25, 35, 45, 55, 65, 100],
        labels=["0-18", "18-25", "25-35", "35-45", "45-55", "55-65", "65+"]
    )
    v2 = (
        df[df["membership"] != "Not Logged In"]
        .groupby(["age_group", "membership"])
        .agg(total_vendas=("sales", "sum"), contagem=("sales", "count"))
        .reset_index()
    )
    resultados_vista2.append(add_particao(v2))

    # VISTA 3 — Vendas e acessos por hora e gênero

    v3_vendas = (
        df.groupby(["hour", "gender"])
        .agg(total_vendas=("sales", "sum"))
        .reset_index()
    )
    resultados_vista3_vendas.append(add_particao(v3_vendas))

    v3_acessos = ( 
        df.groupby("hour")
        .agg(total_acessos=("accessed_date", "count"))
        .reset_index()
    )
    resultados_vista3_acessos.append(add_particao(v3_acessos))

    # VISTA 4 — Correlação entre variáveis numéricas

    df_num = df.select_dtypes(include="number")
    if not df_num.empty:
        v4 = df_num.corr().reset_index().rename(columns={"index": "variavel"})
        resultados_vista4.append(add_particao(v4))

# SALVA TODAS AS VISTAS

pd.concat(resultados_vista1, ignore_index=True)\
    .to_csv("vistas/vendas_por_pais_e_pagamento.csv", index=False)
print("Vista 1 salva!")

pd.concat(resultados_vista2, ignore_index=True)\
    .to_csv("vistas/vendas_por_faixa_etaria_membership.csv", index=False)
print("Vista 2 salva!")

v3_vendas_final = pd.concat(resultados_vista3_vendas, ignore_index=True)
v3_acessos_final = pd.concat(resultados_vista3_acessos, ignore_index=True)
v3_vendas_final.merge(v3_acessos_final, on=["hour", "ano", "mes", "dia", "hora"], how="left")\
    .to_csv("vistas/vendas_acessos_por_hora_genero.csv", index=False)
print("Vista 3 salva!")

pd.concat(resultados_vista4, ignore_index=True)\
    .to_csv("vistas/correlacao_variaveis_numericas.csv", index=False)
print("Vista 4 salva!")
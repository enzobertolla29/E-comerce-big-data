import pandas as pd
import os
import glob

arquivos = glob.glob("dados_particionados/**/*.csv", recursive=True)
df = pd.concat([pd.read_csv(f) for f in arquivos], ignore_index=True)

df["accessed_date"] = pd.to_datetime(df["accessed_date"])
df["hour"] = df["accessed_date"].dt.hour

os.makedirs("vistas", exist_ok=True)

# VISTA 1 — Vendas por país

vista1 = (
    df.groupby(["country", "pay_method"])
    .agg(contagem=("sales", "count"), total_vendas=("sales", "sum"))
    .reset_index()
)
vista1.to_csv("vistas/vendas_por_pais_e_pagamento.csv", index=False)
print("Vista 1 salva: vendas_por_pais_e_pagamento.csv")

# VISTA 2 — Vendas por faixa etária e membership

df["age_group"] = pd.cut(
    pd.to_numeric(df["age"], errors="coerce"),
    bins=[0, 18, 25, 35, 45, 55, 65, 100],
    labels=["0-18", "18-25", "25-35", "35-45", "45-55", "55-65", "65+"]
)

vista2 = (
    df[df["membership"] != "Not Logged In"]
    .groupby(["age_group", "membership"])
    .agg(total_vendas=("sales", "sum"), contagem=("sales", "count"))
    .reset_index()
)
vista2.to_csv("vistas/vendas_por_faixa_etaria_membership.csv", index=False)
print("Vista 2 salva: vendas_por_faixa_etaria_membership.csv")

# VISTA 3 — Vendas e acessos por hora e gênero

vista3_vendas = (
    df.groupby(["hour", "gender"])
    .agg(total_vendas=("sales", "sum"))
    .reset_index()
)

vista3_acessos = (
    df.groupby("hour")
    .agg(total_acessos=("accessed_date", "count"))
    .reset_index()
)

vista3 = vista3_vendas.merge(vista3_acessos, on="hour", how="left")
vista3.to_csv("vistas/vendas_acessos_por_hora_genero.csv", index=False)
print("Vista 3 salva: vendas_acessos_por_hora_genero.csv")

# VISTA 4 — Correlação entre variáveis numéricas

df_num = df.select_dtypes(include="number")
vista4 = df_num.corr().reset_index().rename(columns={"index": "variavel"})
vista4.to_csv("vistas/correlacao_variaveis_numericas.csv", index=False)
print("Vista 4 salva: correlacao_variaveis_numericas.csv")
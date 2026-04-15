import pandas as pd
import os

# 1. Lê a base bruta
df = pd.read_csv("E-commerce Website Logs.csv")

# 2. Converte a coluna de data
df["timestamp"] = pd.to_datetime(df["accessed_date"])

# 3. Extrai as partições de tempo
df["ano"]  = df["timestamp"].dt.year
df["mes"]  = df["timestamp"].dt.month
df["dia"]  = df["timestamp"].dt.day
df["hora"] = df["timestamp"].dt.hour

# 4. Particiona e salva localmente
for (ano, mes, dia, hora), grupo in df.groupby(["ano", "mes", "dia", "hora"]):
    caminho = f"dados_particionados/ano={ano}/mes={mes:02d}/dia={dia:02d}/hora={hora:02d}"
    os.makedirs(caminho, exist_ok=True)
    grupo.to_csv(f"{caminho}/dados.csv", index=False)
    print(f"Salvo: {caminho}")
import pandas as pd
import requests
from datetime import datetime
import pytz
import streamlit as st

# ======================================================
# FILTRO DE FECHAS
# ======================================================
st.title("Telemetría de Recorridos por NIA")

# Selector de rango de fechas
fecha_inicio = st.date_input("Fecha de inicio", value=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0))
fecha_fin = st.date_input("Fecha de fin", value=datetime.now())

# Convertir a timestamps en milisegundos
start_ts = int(datetime(fecha_inicio.year, fecha_inicio.month, fecha_inicio.day, 0, 0, 0).timestamp() * 1000)
end_ts = int(datetime(fecha_fin.year, fecha_fin.month, fecha_fin.day, 23, 59, 59).timestamp() * 1000)

st.write(f"Mostrando datos desde {fecha_inicio} hasta {fecha_fin}")


# ======================================================
# CONFIGURACIÓN
# ======================================================
BASE_URL = "https://tracker.acerosarequipa.com"
USERNAME = "demo.aceria@smelpro.com"
PASSWORD = "demo2025"
ASSET_ID = "00ad3a40-838f-11f0-97ae-99ce2c54259f"

KEYS = [
    "logs_nia",
    "logs_ubicacion",
    "shared_placaTracto","shared_placaPlataforma",
    "shared_tracker","shared_dni","shared_conductor","shared_empresa","shared_ruc",
]

# Últimos 10 días
#end_ts = int(datetime.now().timestamp() * 1000)
#start_ts = end_ts - 10*(24*60*60*1000)

# ======================================================
# LOGIN Y SESIÓN
# ======================================================
session = requests.Session()
login = session.post(
    f"{BASE_URL}/api/auth/login",
    json={"username": USERNAME, "password": PASSWORD},
    timeout=15
)
login.raise_for_status()
token = login.json()["token"]
session.headers.update({
    "X-Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
})

# ======================================================
# TELEMETRÍA
# ======================================================
url = (
    f"{BASE_URL}/api/plugins/telemetry/ASSET/{ASSET_ID}/values/timeseries"
    f"?keys={','.join(KEYS)}"
    f"&startTs={start_ts}&endTs={end_ts}"
    f"&agg=NONE&order=ASC&limit=100000"
)
resp = session.get(url, timeout=30)
resp.raise_for_status()
data = resp.json()
if not data:
    st.error("No se encontraron eventos")

# ======================================================
# NORMALIZAR JSON A DATAFRAME
# ======================================================
dfs = []
for key, values in data.items():
    if not values:
        continue
    df_key = pd.DataFrame(values)
    df_key.rename(columns={"value": key}, inplace=True)
    dfs.append(df_key[["ts", key]])
df_all = pd.concat(dfs).groupby("ts", as_index=False).first()
df_all.rename(columns={"ts": "evento_ts"}, inplace=True)

# Convertir UTC a hora local Perú
df_all["evento_fecha"] = (
    pd.to_datetime(df_all["evento_ts"], unit="ms", utc=True)
          .dt.tz_convert("America/Lima")
          .dt.tz_localize(None)
)

# ======================================================
# LIMPIEZA Y RELLENO
# ======================================================
df = df_all.copy()
df["evento_ts"] = pd.to_numeric(df["evento_ts"], errors="coerce")
df = df.dropna(subset=["logs_nia", "evento_ts"])

cols_a_rellenar = [
    "shared_placaTracto",
    "shared_placaPlataforma",
    "shared_tracker",
    "shared_dni",
    "shared_conductor",
    "shared_empresa",
    "shared_ruc"
]
df_desasig = df[df["logs_ubicacion"] == "Desasignación"][["logs_nia"] + cols_a_rellenar].drop_duplicates(subset="logs_nia")
df = df.merge(df_desasig, on="logs_nia", how="left", suffixes=('', '_desasig'))
for col in cols_a_rellenar:
    df[col] = df[col].fillna(df[f"{col}_desasig"])
df.drop(columns=[f"{col}_desasig" for col in cols_a_rellenar], inplace=True)

# ======================================================
# FILTRAR NIA CON RECORRIDO COMPLETO
# ======================================================
def recorrido_completo(t: pd.DataFrame) -> bool:
    ts_ingreso = t.loc[t["logs_ubicacion"] == "En Asignación", "evento_ts"]
    ts_salida  = t.loc[t["logs_ubicacion"] == "Desasignación", "evento_ts"]
    return (
        not ts_ingreso.empty
        and not ts_salida.empty
        and ts_ingreso.min() < ts_salida.max()
    )

nias_validos = df.groupby("logs_nia", sort=False).filter(recorrido_completo)["logs_nia"].unique()
df = df[df["logs_nia"].isin(nias_validos)]

# ======================================================
# CALCULAR TIEMPOS DE PERMANENCIA
# ======================================================
tz_pe = pytz.timezone("America/Lima")
def tiempos_asig_desasig(t: pd.DataFrame):
    ts_ingreso = t.loc[t["logs_ubicacion"] == "En Asignación", "evento_ts"]
    ts_salida  = t.loc[t["logs_ubicacion"] == "Desasignación", "evento_ts"]
    if not ts_ingreso.empty and not ts_salida.empty:
        ts_entrada = ts_ingreso.min()
        ts_salida_max = ts_salida.max()
        return pd.Series({
            "tiempo_permanencia": (ts_salida_max - ts_entrada)/1000/3600,
            "ingreso": pd.to_datetime(ts_entrada, unit='ms').tz_localize('UTC').tz_convert(tz_pe),
            "salida": pd.to_datetime(ts_salida_max, unit='ms').tz_localize('UTC').tz_convert(tz_pe)
        })
    else:
        return pd.Series({
            "tiempo_permanencia": None,
            "ingreso": None,
            "salida": None
        })
df_tiempos = df.groupby("logs_nia", group_keys=False).apply(tiempos_asig_desasig).reset_index()

# ======================================================
# ORDEN Y CALCULO DE TIEMPOS ENTRE EVENTOS
# ======================================================
df = df.sort_values(["logs_nia", "evento_ts"]).assign(
    evento_ts_siguiente=lambda x: x.groupby("logs_nia")["evento_ts"].shift(-1),
    tiempo_min=lambda x: (x["evento_ts_siguiente"] - x["evento_ts"])/1000/60
)
df = df[df["tiempo_min"].notna() & (df["tiempo_min"] >= 0)]

# ======================================================
# RENOMBRAR BALANZA INICIAL/FINAL
# ======================================================
df["logs_ubicacion_renombrada"] = df["logs_ubicacion"]
for nia, grupo in df.groupby("logs_nia"):
    grupo = grupo.sort_values("evento_ts")
    balanza = grupo[grupo["logs_ubicacion"] == "Balanza"]
    if not balanza.empty:
        balanza_ini_idx = balanza.index[0]
        balanza_fin_idx = balanza.index[-1]
        df.loc[balanza_ini_idx, "logs_ubicacion_renombrada"] = "Balanza inicial"
        df.loc[balanza_fin_idx, "logs_ubicacion_renombrada"] = "Balanza final"

        ruta_ini = grupo[grupo["evento_ts"] < grupo.loc[balanza_ini_idx, "evento_ts"]]
        if not ruta_ini.empty:
            ruta_ini_idx = ruta_ini.index[-1]
            if grupo.loc[ruta_ini_idx, "logs_ubicacion"] == "Ruta hacia Balanza":
                df.loc[ruta_ini_idx, "logs_ubicacion_renombrada"] = "Ruta hacia Balanza inicial"

        ruta_fin = grupo[grupo["evento_ts"] < grupo.loc[balanza_fin_idx, "evento_ts"]]
        if not ruta_fin.empty:
            ruta_fin_idx = ruta_fin.index[-1]
            if grupo.loc[ruta_fin_idx, "logs_ubicacion"] == "Ruta hacia Balanza":
                df.loc[ruta_fin_idx, "logs_ubicacion_renombrada"] = "Ruta hacia Balanza final"

# ======================================================
# RESULTADO FINAL Y PIVOT
# ======================================================
df_final = df.drop(columns=["evento_ts_siguiente"]).sort_values(["logs_nia", "evento_ts"])
df_final = df_final.merge(df_tiempos, on="logs_nia", how="left")

df_pivot = df_final.groupby(["logs_nia", "logs_ubicacion_renombrada"])["tiempo_min"].sum().reset_index()
df_pivot_final = df_pivot.pivot(index="logs_nia", columns="logs_ubicacion_renombrada", values="tiempo_min").reset_index()
df_pivot_final = df_pivot_final.merge(df_tiempos, on="logs_nia", how="left")

cols_descarga = [
    "Balanza","Balanza final","Balanza inicial","Barrido",
    "Calificacion","Calificación","Consumo","Desasignación",
    "Descarga","Desmanteo","Embutición","Iman Core","Imán",
    "Oxicorte","Ruta hacia Balanza","Ruta hacia Balanza final",
    "Ruta hacia Balanza inicial","Ruta hacia Barrido",
    "Ruta hacia Calificacion","Ruta hacia Calificación",
    "Ruta hacia Consumo","Ruta hacia Descarga",
    "Ruta hacia Desmanteo","Ruta hacia Embutición",
    "Ruta hacia Imán","Ruta hacia Oxicorte"
]
cols_existentes = [c for c in cols_descarga if c in df_pivot_final.columns]
df_pivot_final["tiempo_descarga"] = df_pivot_final[cols_existentes].sum(axis=1)

# ======================================================
# MOSTRAR EN STREAMLIT
# ======================================================
st.title("Telemetría de Recorridos por NIA")
st.dataframe(df_pivot_final)

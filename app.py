import streamlit as st
import requests
import pandas as pd
from datetime import datetime
from streamlit_autorefresh import st_autorefresh

# ======================================================
# CONFIG STREAMLIT
# ======================================================
st.set_page_config(
    page_title="Recorridos Completos",
    layout="wide"
)

st.title("🚚 Dashboard de Recorridos Completos")
st.markdown("Datos en tiempo real desde el tracker")

# Auto refresh cada 60 segundos
st_autorefresh(interval=60_000, key="refresh")

# ======================================================
# CONFIGURACIÓN API
# ======================================================
BASE_URL = "https://tracker.acerosarequipa.com"
USERNAME = "demo.aceria@smelpro.com"
PASSWORD = "demo2025"

ASSET_ID = "00ad3a40-838f-11f0-97ae-99ce2c54259f"

KEYS = [
    "logs_ubicacion",
    "fechaAsignacion","fechaDesasignacion",
    "shared_placaTracto","shared_placaPlataforma",
    "shared_conductor","shared_empresa",
    "balanzaTime","desmanteoTime","calificacionTime",
    "descargaTime","imanTime","barridoTime",
    "oxicorteTime","embuticionTime","consumoTime"
]

# Últimas 3 horas
end_ts = int(datetime.now().timestamp() * 1000)
start_ts = end_ts - 60*(24 * 60 * 60 * 1000)

# ======================================================
# FUNCIONES
# ======================================================
@st.cache_data(ttl=60)
def obtener_datos():
    session = requests.Session()

    # LOGIN
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

    # TELEMETRÍA
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
        return pd.DataFrame()

    # NORMALIZAR JSON → TABLA
    dfs = []
    for key, values in data.items():
        if values:
            df = pd.DataFrame(values)
            df.rename(columns={"value": key}, inplace=True)
            dfs.append(df[["ts", key]])

    df_all = (
        pd.concat(dfs)
        .groupby("ts", as_index=False)
        .first()
        .rename(columns={"ts": "evento_ts"})
    )

    # Fecha local Perú
    df_all["evento_fecha"] = (
        pd.to_datetime(df_all["evento_ts"], unit="ms", utc=True)
          .dt.tz_convert("America/Lima")
          .dt.tz_localize(None)
    )

    return df_all.sort_values("evento_fecha").reset_index(drop=True)

# ======================================================
# PROCESAMIENTO
# ======================================================
df = obtener_datos()

if df.empty:
    st.warning("No se encontraron datos")
    st.stop()

# ---------------------------
# Asegurar columnas dinámicas
# ---------------------------
COLUMNAS_ESPERADAS = [
    "logs_ubicacion",
    "shared_placaTracto",
    "shared_placaPlataforma",
    "shared_conductor",
    "shared_empresa",
    "balanzaTime",
    "desmanteoTime",
    "calificacionTime",
    "descargaTime",
    "imanTime",
    "barridoTime",
    "oxicorteTime",
    "embuticionTime",
    "consumoTime"
]

for col in COLUMNAS_ESPERADAS:
    if col not in df.columns:
        df[col] = None

# ---------------------------
# Identificar recorridos
# ---------------------------
df["fin_recorrido_flag"] = (
    df["logs_ubicacion"]
    .fillna("")
    .str.lower()
    .eq("desasignación")
)

df["recorrido_id"] = df["fin_recorrido_flag"].cumsum()

# ---------------------------
# Tabla de recorridos
# ---------------------------
recorridos = (
    df.groupby("recorrido_id")
    .agg(
        inicio=("evento_fecha", "min"),
        fin=("evento_fecha", "max"),
        placa_tracto=("shared_placaTracto", "first"),
        placa_plataforma=("shared_placaPlataforma", "first"),
        conductor=("shared_conductor", "first"),
        empresa=("shared_empresa", "first"),
        balanza=("balanzaTime", "first"),
        desmanteo=("desmanteoTime", "first"),
        calificacion=("calificacionTime", "first"),
        descarga=("descargaTime", "first"),
        iman=("imanTime", "first"),
        barrido=("barridoTime", "first"),
        oxicorte=("oxicorteTime", "first"),
        embuticion=("embuticionTime", "first"),
        consumo=("consumoTime", "first")
    )
    .reset_index()
)

# Duración total del recorrido
recorridos["duracion_min"] = (
    (recorridos["fin"] - recorridos["inicio"])
    .dt.total_seconds() / 60
).round(1)

# Solo recorridos cerrados válidos
recorridos = recorridos[recorridos["duracion_min"] > 0]

# ===========================
# INTERFAZ STREAMLIT
# ===========================
st.subheader("📋 Recorridos Completos")

st.dataframe(
    recorridos.sort_values("fin", ascending=False),
    use_container_width=True
)

# KPIs
st.subheader("📊 KPIs")

col1, col2, col3 = st.columns(3)
col1.metric("Total recorridos", len(recorridos))
col2.metric(
    "Duración promedio (min)",
    round(recorridos["duracion_min"].mean(), 1)
)
col3.metric(
    "Última actualización",
    datetime.now().strftime("%H:%M:%S")
)

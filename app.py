import pandas as pd
import requests
from datetime import datetime, timedelta, time
import pytz
import streamlit as st
import plotly.express as px


# CSS para ocultar header, toolbar y footer
hide_header_style = """
    <style>
    /* Oculta todo el header de Streamlit */
    header.stAppHeader {display: none;}
    
    /* Opcional: oculta el footer de Streamlit */
    footer {visibility: hidden;}
    
    /* Opcional: elimina margen superior para que la app suba completamente */
    .stApp {padding-top: 0rem;}
    </style>
"""
st.markdown(hide_header_style, unsafe_allow_html=True)

# CSS más robusto para ocultar toda la UI extra de Streamlit Cloud
hide_streamlit_ui = """
<style>
/* Oculta header completo */
header.stAppHeader {display: none !important;}

/* Oculta footer */
footer {display: none !important;}

/* Oculta toolbar flotante */
div[data-testid="stToolbar"] {display: none !important;}

/* Oculta badges de Streamlit Cloud (logo y perfil) */
a[href*="streamlit.io"], div[data-testid="appCreatorAvatar"] {display: none !important;}

/* Elimina padding superior */
.stApp {padding-top: 0rem !important;}
</style>
"""
st.markdown(hide_streamlit_ui, unsafe_allow_html=True)
# ======================================================
# CAMBIAR COLOR DE FONDO
# ======================================================
page_bg_color = "#b8c3d9"  # Cambia este color a tu gusto
st.markdown(
    f"""
    <style>
    .stApp {{
        background-color: {page_bg_color};
    }}
    </style>
    """,
    unsafe_allow_html=True
)

# ======================================================
# CONFIGURACIÓN STREAMLIT
# ======================================================
st.set_page_config(
    page_title="Telemetría NIA",
    layout="wide",
    initial_sidebar_state="expanded"
)
st.title("Telemetría de Recorridos por NIA")

# ======================================================
# ZONA HORARIA Y FECHA
# ======================================================
tz_pe = pytz.timezone("America/Lima")
now_pe = datetime.now(tz_pe)

# ======================================================
# FILTRO DE FECHAS
# ======================================================
opciones_rango = ["Custom", "Últimas horas", "Último turno", "Turno actual"]
seleccion_rango = st.selectbox("Seleccione tipo de filtrado", opciones_rango)

start_dt = end_dt = None

if seleccion_rango == "Últimas horas":
    ultimas_horas_opciones = ["1 hora", "3 horas", "6 horas", "12 horas", "24 horas", "48 horas"]
    seleccion_horas = st.selectbox("Seleccione últimas horas", ultimas_horas_opciones)
    horas = int(seleccion_horas.split()[0])
    start_dt = now_pe - timedelta(hours=horas)
    end_dt = now_pe

elif seleccion_rango in ["Último turno", "Turno actual"]:
    hora_actual = now_pe.time()
    fecha_actual = now_pe.date()

    if seleccion_rango == "Último turno":
        if time(8,0) <= hora_actual < time(20,0):
            start_dt = tz_pe.localize(datetime.combine(fecha_actual - timedelta(days=1), time(20,0)))
            end_dt = tz_pe.localize(datetime.combine(fecha_actual, time(8,0)))
        elif hora_actual >= time(20,0):
            start_dt = tz_pe.localize(datetime.combine(fecha_actual, time(8,0)))
            end_dt = tz_pe.localize(datetime.combine(fecha_actual, time(20,0)))
        else:
            start_dt = tz_pe.localize(datetime.combine(fecha_actual - timedelta(days=1), time(8,0)))
            end_dt = tz_pe.localize(datetime.combine(fecha_actual - timedelta(days=1), time(20,0)))
    else:
        if time(8,0) <= hora_actual < time(20,0):
            start_dt = tz_pe.localize(datetime.combine(fecha_actual, time(8,0)))
            end_dt = tz_pe.localize(datetime.combine(fecha_actual, time(20,0)))
        else:
            if hora_actual >= time(20,0):
                start_dt = tz_pe.localize(datetime.combine(fecha_actual, time(20,0)))
                end_dt = tz_pe.localize(datetime.combine(fecha_actual + timedelta(days=1), time(8,0)))
            else:
                start_dt = tz_pe.localize(datetime.combine(fecha_actual - timedelta(days=1), time(20,0)))
                end_dt = tz_pe.localize(datetime.combine(fecha_actual, time(8,0)))

else:
    start_dt = st.datetime_input("Fecha y hora de inicio", value=now_pe - timedelta(hours=1))
    end_dt = st.datetime_input("Fecha y hora de fin", value=now_pe)

start_ts = int(start_dt.astimezone(pytz.UTC).timestamp() * 1000)
end_ts = int(end_dt.astimezone(pytz.UTC).timestamp() * 1000)

st.write(f"Mostrando datos desde: {start_dt.strftime('%Y-%m-%d %H:%M:%S')} hasta {end_dt.strftime('%Y-%m-%d %H:%M:%S')} (Hora Perú)")
st.write(f"Timestamps UTC para API: {start_ts} - {end_ts}")

# ======================================================
# CONFIGURACIÓN API
# ======================================================
BASE_URL = "https://tracker.acerosarequipa.com"
USERNAME = "demo.aceria@smelpro.com"
PASSWORD = "demo2025"
ASSET_ID = "00ad3a40-838f-11f0-97ae-99ce2c54259f"
KEYS = [
    "logs_nia","logs_ubicacion",
    "shared_placaTracto","shared_placaPlataforma",
    "shared_tracker","shared_dni","shared_conductor",
    "shared_empresa","shared_ruc"
]

# ======================================================
# LOGIN Y SESIÓN
# ======================================================
session = requests.Session()
login = session.post(f"{BASE_URL}/api/auth/login",
                     json={"username": USERNAME, "password": PASSWORD}, timeout=15)
login.raise_for_status()
token = login.json().get("token")
session.headers.update({"X-Authorization": f"Bearer {token}", "Content-Type": "application/json"})

# ======================================================
# OBTENER TELEMETRÍA
# ======================================================
url = (
    f"{BASE_URL}/api/plugins/telemetry/ASSET/{ASSET_ID}/values/timeseries"
    f"?keys={','.join(KEYS)}&startTs={start_ts}&endTs={end_ts}"
    "&agg=NONE&order=ASC&limit=100000"
)
resp = session.get(url, timeout=30)
resp.raise_for_status()
data = resp.json()

if not data or all(not v for v in data.values()):
    st.warning("No se encontraron eventos en el rango seleccionado.")
    st.stop()

# ======================================================
# NORMALIZAR JSON A DATAFRAME
# ======================================================
dfs = []
for key, values in data.items():
    if values:
        df_key = pd.DataFrame(values).rename(columns={"value": key})
        dfs.append(df_key[["ts", key]])
df_all = pd.concat(dfs, ignore_index=True).groupby("ts", as_index=False).first().rename(columns={"ts":"evento_ts"})
df_all["evento_fecha"] = pd.to_datetime(df_all["evento_ts"], unit="ms", utc=True).dt.tz_convert(tz_pe).dt.tz_localize(None)
df = df_all.copy()

# ======================================================
# LIMPIEZA Y RELLENO CON COLUMNAS EXISTENTES
# ======================================================
df = df.dropna(subset=[c for c in ["logs_nia","logs_ubicacion"] if c in df.columns])

cols_a_rellenar = ["shared_placaTracto","shared_placaPlataforma","shared_tracker",
                   "shared_dni","shared_conductor","shared_empresa","shared_ruc"]

# Columnas reales disponibles
cols_reales = [c for c in ["logs_nia"] + cols_a_rellenar if c in df.columns]

# Filtrar solo si "logs_ubicacion" existe
if "logs_ubicacion" in df.columns:
    df_desasig = df[df["logs_ubicacion"]=="Desasignación"][cols_reales].drop_duplicates(subset="logs_nia")
    for col in cols_a_rellenar:
        if col in df.columns and f"{col}_desasig" not in df.columns:
            df = df.merge(df_desasig[["logs_nia", col]], on="logs_nia", how="left", suffixes=('', '_desasig'))
            df[col] = df[col].fillna(df[f"{col}_desasig"])
            df.drop(columns=[f"{col}_desasig"], inplace=True)

# ======================================================
# FILTRAR NIA CON RECORRIDO COMPLETO
# ======================================================
def recorrido_completo(gr):
    if "logs_ubicacion" not in gr.columns:
        return False
    ts_ing = gr.loc[gr["logs_ubicacion"]=="En Asignación", "evento_ts"]
    ts_sal = gr.loc[gr["logs_ubicacion"]=="Desasignación", "evento_ts"]
    return not ts_ing.empty and not ts_sal.empty and ts_ing.min() < ts_sal.max()

if "logs_nia" in df.columns:
    nias_validos = df.groupby("logs_nia").filter(recorrido_completo)["logs_nia"].unique()
    df = df[df["logs_nia"].isin(nias_validos)]

# ======================================================
# TIEMPOS DE PERMANENCIA
# ======================================================
if "logs_nia" in df.columns:
    agg_df = df.groupby("logs_nia").agg(
        ts_ingreso=("evento_ts", lambda x: x[df.loc[x.index,"logs_ubicacion"]=="En Asignación"].min() if "logs_ubicacion" in df.columns else pd.NA),
        ts_salida=("evento_ts", lambda x: x[df.loc[x.index,"logs_ubicacion"]=="Desasignación"].max() if "logs_ubicacion" in df.columns else pd.NA)
    ).reset_index()

    agg_df["tiempo_permanencia"] = (agg_df["ts_salida"] - agg_df["ts_ingreso"])/1000/3600
    agg_df["ingreso"] = pd.to_datetime(agg_df["ts_ingreso"], unit='ms', errors='coerce').dt.tz_localize('UTC').dt.tz_convert(tz_pe)
    agg_df["salida"] = pd.to_datetime(agg_df["ts_salida"], unit='ms', errors='coerce').dt.tz_localize('UTC').dt.tz_convert(tz_pe)
else:
    agg_df = pd.DataFrame(columns=["logs_nia","ts_ingreso","ts_salida","tiempo_permanencia","ingreso","salida"])

# ======================================================
# TIEMPO ENTRE EVENTOS
# ======================================================
if "logs_nia" in df.columns:
    df = df.sort_values(["logs_nia","evento_ts"])
    df["evento_ts_siguiente"] = df.groupby("logs_nia")["evento_ts"].shift(-1)
    df["tiempo_min"] = (df["evento_ts_siguiente"] - df["evento_ts"])/1000/60
    df = df[df["tiempo_min"].notna() & (df["tiempo_min"]>=0)]

# ======================================================
# RENOMBRAR BALANZA INICIAL/FINAL
# ======================================================
df["logs_ubicacion_renombrada"] = df.get("logs_ubicacion", pd.Series([""]*len(df)))
# Aquí se puede agregar la lógica de renombrado si existen los datos de Balanza

# ======================================================
# PIVOT FINAL
# ======================================================
df_final = df.drop(columns=["evento_ts_siguiente"], errors='ignore').sort_values(["logs_nia","evento_ts"])
df_final = df_final.merge(agg_df, on="logs_nia", how="left")

# Columnas de descarga
cols_descarga = [
    "Balanza","Balanza final","Balanza inicial","Barrido","Calificacion","Calificación",
    "Consumo","Desasignación","Descarga","Desmanteo","Embutición","Iman Core","Imán",
    "Oxicorte","Ruta hacia Balanza","Ruta hacia Balanza final","Ruta hacia Balanza inicial",
    "Ruta hacia Barrido","Ruta hacia Calificacion","Ruta hacia Calificación",
    "Ruta hacia Consumo","Ruta hacia Descarga","Ruta hacia Desmanteo",
    "Ruta hacia Embutición","Ruta hacia Imán","Ruta hacia Oxicorte"
]
df_pivot_final = df_final.groupby(["logs_nia","logs_ubicacion_renombrada"])["tiempo_min"].sum().reset_index()
df_pivot_final = df_pivot_final.pivot(index="logs_nia", columns="logs_ubicacion_renombrada", values="tiempo_min").reset_index()
df_pivot_final = df_pivot_final.merge(agg_df, on="logs_nia", how="left")
df_pivot_final["tiempo_descarga"] = df_pivot_final.filter(items=cols_descarga).sum(axis=1)

# ======================================================
# MOSTRAR TABLA EN STREAMLIT
# ======================================================
st.subheader("Tabla de tiempos por NIA")
st.dataframe(df_pivot_final, use_container_width=True)

# ======================================================
# GRAFICOS
# ======================================================
st.subheader("Visualización de tiempos promedio por ubicación")
if not df_final.empty and "logs_ubicacion_renombrada" in df_final.columns:
    prom_ubicacion = df_final.groupby("logs_ubicacion_renombrada")["tiempo_min"].mean().reset_index()
    prom_ubicacion.rename(columns={"tiempo_min":"promedio_minutos"}, inplace=True)

    ubicaciones = prom_ubicacion['logs_ubicacion_renombrada'].tolist()
    selected_location = st.selectbox("Seleccione ubicación", ubicaciones)

    nias_filtradas = df_final[df_final["logs_ubicacion_renombrada"]==selected_location][["logs_nia","tiempo_min"]].drop_duplicates()

    st.write(f"Tiempo promedio en {selected_location}: {prom_ubicacion.loc[prom_ubicacion['logs_ubicacion_renombrada']==selected_location,'promedio_minutos'].values[0]:.2f} minutos")
    st.dataframe(nias_filtradas, use_container_width=True)

    prom_ubicacion["highlight"] = prom_ubicacion["logs_ubicacion_renombrada"] == selected_location
    fig = px.bar(
        prom_ubicacion,
        x="logs_ubicacion_renombrada",
        y="promedio_minutos",
        color="highlight",
        color_discrete_map={True:"orange", False:"steelblue"},
        labels={"logs_ubicacion_renombrada":"Ubicación","promedio_minutos":"Tiempo promedio (minutos)"}
    )
    fig.update_layout(title="Tiempo promedio por ubicación", xaxis_title="Ubicación", yaxis_title="Tiempo promedio (minutos)")
    st.plotly_chart(fig, use_container_width=True)

import pandas as pd
import requests
from datetime import datetime
import pytz
import streamlit as st


# Configurar la página para que sea a lo ancho
st.set_page_config(
    page_title="Telemetría NIA",  # Título de la pestaña
    layout="wide",                 # Layout a lo ancho
    initial_sidebar_state="expanded"  # Opcional: barra lateral abierta por defecto
)


# ======================================================
# FILTRO DE FECHAS
# ======================================================
from datetime import datetime, timedelta, time
import pytz

st.title("Filtro de tiempo para Telemetría")

# Zona horaria Perú
tz_pe = pytz.timezone("America/Lima")
now_pe = datetime.now(tz_pe)

# --- Desplegable de rango rápido ---
opciones_rango = ["Custom", "Últimas horas", "Último turno", "Turno actual"]
seleccion_rango = st.selectbox("Seleccione tipo de filtrado", opciones_rango)

# Inicializamos start_dt y end_dt
start_dt = end_dt = None

if seleccion_rango == "Últimas horas":
    ultimas_horas_opciones = ["1 hora", "3 horas", "6 horas", "12 horas", "24 horas", "48 horas"]
    seleccion_horas = st.selectbox("Seleccione últimas horas", ultimas_horas_opciones)
    horas = int(seleccion_horas.split()[0])
    start_dt = now_pe - timedelta(hours=horas)
    end_dt = now_pe

elif seleccion_rango == "Último turno":
    # Turnos: día 08:00-20:00, noche 20:00-08:00
    hora_actual = now_pe.time()
    fecha_actual = now_pe.date()
    
    if time(8,0) <= hora_actual < time(20,0):
        # Último turno fue nocturno 20:00 del día anterior a 08:00 hoy
        start_dt = tz_pe.localize(datetime.combine(fecha_actual - timedelta(days=1), time(20,0)))
        end_dt = tz_pe.localize(datetime.combine(fecha_actual, time(8,0)))
    elif hora_actual >= time(20,0):
        # Último turno fue día 08:00 a 20:00 hoy
        start_dt = tz_pe.localize(datetime.combine(fecha_actual, time(8,0)))
        end_dt = tz_pe.localize(datetime.combine(fecha_actual, time(20,0)))
    else: # hora_actual < 08:00
        # Último turno fue día 08:00 a 20:00 día anterior
        start_dt = tz_pe.localize(datetime.combine(fecha_actual - timedelta(days=1), time(8,0)))
        end_dt = tz_pe.localize(datetime.combine(fecha_actual - timedelta(days=1), time(20,0)))

elif seleccion_rango == "Turno actual":
    # Turno actual según hora actual
    hora_actual = now_pe.time()
    fecha_actual = now_pe.date()

    if time(8,0) <= hora_actual < time(20,0):
        # Turno día actual
        start_dt = tz_pe.localize(datetime.combine(fecha_actual, time(8,0)))
        end_dt = tz_pe.localize(datetime.combine(fecha_actual, time(20,0)))
    else:
        # Turno noche actual
        if hora_actual >= time(20,0):
            start_dt = tz_pe.localize(datetime.combine(fecha_actual, time(20,0)))
            end_dt = tz_pe.localize(datetime.combine(fecha_actual + timedelta(days=1), time(8,0)))
        else:  # hora_actual < 08:00
            start_dt = tz_pe.localize(datetime.combine(fecha_actual - timedelta(days=1), time(20,0)))
            end_dt = tz_pe.localize(datetime.combine(fecha_actual, time(8,0)))

else:
    # Custom: selector manual de fecha y hora
    start_dt = st.datetime_input("Fecha y hora de inicio", value=now_pe - timedelta(hours=1))
    end_dt = st.datetime_input("Fecha y hora de fin", value=now_pe)

# Convertir a timestamps UTC para la API
start_ts = int(start_dt.astimezone(pytz.UTC).timestamp() * 1000)
end_ts = int(end_dt.astimezone(pytz.UTC).timestamp() * 1000)

st.write(f"Mostrando datos desde: {start_dt.strftime('%Y-%m-%d %H:%M:%S')} hasta {end_dt.strftime('%Y-%m-%d %H:%M:%S')} (Hora Perú)")
st.write(f"Timestamps UTC (para API): start_ts = {start_ts}, end_ts = {end_ts}")


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

df_pivot_final.to_csv("tiempos_tsb_pivot_limpio.csv", index=False)

# ======================================================
# MOSTRAR EN STREAMLIT
# ======================================================

st.title("Telemetría de Recorridos por NIA")
# Mostrar DataFrame con ancho completo
st.dataframe(df_pivot_final, use_container_width=True)
# ======================================================
# MOSTRAR EN STREAMLIT graficas
# ======================================================
import altair as alt
import streamlit as st
import pandas as pd

st.subheader("Tiempo promedio por ubicación con detalle de NIA")

# Ejemplo de df_final
# df_final debe tener: logs_nia, logs_ubicacion_renombrada, tiempo_min

# 1️⃣ Calcular promedio por ubicación
prom_ubicacion = df_final.groupby("logs_ubicacion_renombrada")["tiempo_min"].mean().reset_index()
prom_ubicacion.rename(columns={"tiempo_min": "promedio_minutos"}, inplace=True)

# 2️⃣ Crear selección por clic
selection = alt.selection_single(fields=['logs_ubicacion_renombrada'], empty="none", on="click")

# 3️⃣ Gráfico de barras interactivo
chart_ubicacion = alt.Chart(prom_ubicacion).mark_bar().encode(
    x=alt.X("logs_ubicacion_renombrada:N", title="Ubicación"),
    y=alt.Y("promedio_minutos:Q", title="Tiempo promedio (minutos)"),
    color=alt.condition(selection, alt.value("orange"), alt.value("steelblue")),
    tooltip=["logs_ubicacion_renombrada", "promedio_minutos"]
).add_selection(
    selection
).properties(
    width=800,
    height=400
).interactive()

st.altair_chart(chart_ubicacion)

# 4️⃣ Mostrar los NIA correspondientes a la barra seleccionada
if selection:  # Streamlit necesita filtrar manualmente
    # Obtener la ubicación seleccionada
    loc_sel = st.altair_chart(chart_ubicacion)._chart_dict['selection'] if chart_ubicacion else None

# Más fácil con Streamlit: usar altair `transform_filter` para mostrar tabla
# Creamos tabla filtrada con streamlit usando `selection` de Altair
# Para Streamlit se puede hacer así:
st.subheader("NIA utilizados para el promedio")

# Crear selección como string en Streamlit
selected_location = st.selectbox("Selecciona la ubicación para ver los NIA", prom_ubicacion['logs_ubicacion_renombrada'])

nias_filtradas = df_final[df_final["logs_ubicacion_renombrada"] == selected_location][["logs_nia", "tiempo_min"]].drop_duplicates()
st.dataframe(nias_filtradas, use_container_width=True)



import streamlit as st
import pandas as pd
import plotly.express as px
from streamlit_autorefresh import st_autorefresh
import random
from datetime import datetime

# Auto refresco cada 5 segundos
st_autorefresh(interval=5000, key="datarefresh")

st.set_page_config(page_title="Dashboard en Tiempo Real", layout="wide")

st.title("Dashboard Público en Tiempo Real")
st.markdown("Actualización automática cada 5 segundos (Streamlit Cloud)")

# Generar dato en tiempo real
new_row = pd.DataFrame({
    "Hora": [datetime.now().strftime("%H:%M:%S")],
    "Valor": [random.randint(0, 100)]
})

if "hist" not in st.session_state:
    st.session_state.hist = new_row
else:
    st.session_state.hist = pd.concat([st.session_state.hist, new_row]).tail(20)

col1, col2 = st.columns([2,1])

with col1:
    fig = px.line(st.session_state.hist, x="Hora", y="Valor", markers=True)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Últimos valores")
    st.dataframe(st.session_state.hist, use_container_width=True)


import os

import pandas as pd
import streamlit as st
from pymongo import MongoClient
from pymongo.errors import PyMongoError


st.set_page_config(
    page_title="SID | Monitoreo Logístico",
    page_icon="📦",
    layout="wide"
)

st.title("Sistema de Monitoreo Logístico")
st.caption("Proyecto Final — Sistemas Intensivos de Datos")


@st.cache_resource
def get_database(uri):
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    return client["sid_logistica"]


mongo_uri = os.getenv("MONGO_URI")

if not mongo_uri:
    st.error("Debes configurar la variable MONGO_URI.")
    st.stop()

try:
    db = get_database(mongo_uri)
    shipments = list(
        db["shipments_current"].find({}, {"_id": 0})
    )
except PyMongoError as error:
    st.error(f"No fue posible consultar MongoDB: {error}")
    st.stop()


if not shipments:
    st.warning("Todavía no existen envíos registrados.")
    st.stop()


df = pd.DataFrame(shipments)

# Indicadores generales
total = len(df)
delivered = int((df["status"] == "delivered").sum())
in_transit = int((df["status"] == "in_transit").sum())
alerts = int(df["active_alert"].fillna(False).sum())

st.subheader("Indicadores operacionales")

col1, col2, col3, col4 = st.columns(4)

col1.metric("Total de envíos", total)
col2.metric("Entregados", delivered)
col3.metric("En tránsito", in_transit)
col4.metric("Alertas activas", alerts)

st.divider()

# Búsqueda por número de guía
st.subheader("Consultar envío")

shipment_id = st.text_input(
    "Número de guía",
    placeholder="Ejemplo: ENV-PRUEBA-001"
)

if shipment_id:
    result = df[
        df["shipment_id"] == shipment_id.strip()
    ]

    if result.empty:
        st.warning("No se encontró el envío.")
    else:
        st.dataframe(
            result,
            use_container_width=True,
            hide_index=True
        )

st.divider()

# Monitoreo de riesgos
st.subheader("Monitoreo de riesgos")

risk_counts = (
    df["risk_level"]
    .value_counts()
    .reindex(["low", "medium", "high"], fill_value=0)
)

st.bar_chart(risk_counts)

# Tabla general
st.subheader("Estado actual de los envíos")

columns = [
    "shipment_id",
    "vehicle_id",
    "route_id",
    "status",
    "risk_level",
    "active_alert",
    "estimated_minutes"
]

st.dataframe(
    df[columns],
    use_container_width=True,
    hide_index=True
)

if st.button("Actualizar datos"):
    st.rerun()

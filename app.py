import streamlit as st
import pandas as pd
from datetime import datetime
import os
import altair as alt
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
import numpy as np

# ================= CONFIGURACIÓN =================
DATA_FILE = "datos_analiticas.csv"
DATA_DIR = "data"
REPORT_DIR = "reports/informes_pdf"

os.makedirs(REPORT_DIR, exist_ok=True)

PUNTOS = ["Entrada Planta", "X-507", "Salida FCA"]
PARAMETROS = ["HC", "SS", "DQO", "Sulf"]

LIMITES = {
    "HC": {"puntual": 15, "anual": 2.5},
    "DQO": {"puntual": 700, "anual": 125}
}

st.set_page_config(page_title="Control Analíticas Planta", layout="wide")
st.title("💧 Control de analíticas – Planta de tratamiento de aguas")

# ================= CARGA DE DATOS =================
if os.path.exists(DATA_FILE):
    df = pd.read_csv(DATA_FILE, parse_dates=["datetime"])
else:
    df = pd.DataFrame(columns=[
        "datetime", "punto", "HC", "SS", "DQO", "Sulf", "envio_emisario"
    ])

# ================= IMPORTACIÓN EXCEL =================
st.subheader("📥 Importar datos desde Excel")

if st.button("Importar archivos desde /data"):
    archivos = {
        "entrada_planta.xlsx": "Entrada Planta",
        "x507.xlsx": "X-507",
        "salidafca.xlsx": "Salida FCA"
    }

    nuevos = []

    for archivo, punto in archivos.items():
        ruta = os.path.join(DATA_DIR, archivo)
        if not os.path.exists(ruta):
            continue

        xls = pd.read_excel(
            ruta,
            engine="openpyxl",
            usecols="C:H",
            names=["Fecha", "Hora", "HC", "SS", "DQO", "Sulf"],
            header=None
        )

        for _, r in xls.iterrows():
            if pd.isna(r["HC"]) and pd.isna(r["DQO"]):
                continue

            try:
                dt = datetime.combine(
                    pd.to_datetime(r["Fecha"]).date(),
                    pd.to_datetime(r["Hora"]).time()
                )
            except Exception:
                continue

            nuevos.append({
                "datetime": dt,
                "punto": punto,
                "HC": r["HC"],
                "SS": r["SS"],
                "DQO": r["DQO"],
                "Sulf": r["Sulf"],
                "envio_emisario": False
            })

    if nuevos:
        df = pd.concat([df, pd.DataFrame(nuevos)], ignore_index=True)
        df.to_csv(DATA_FILE, index=False)
        st.success(f"Importados {len(nuevos)} registros")
    else:
        st.info("No se importaron datos")

# ================= ENTRADA MANUAL =================
st.subheader("📝 Introducción manual")

with st.form("form_manual"):
    c1, c2, c3 = st.columns(3)

    with c1:
        fecha = st.date_input("Fecha")
        hora = st.time_input("Hora")

    with c2:
        punto = st.selectbox("Punto", PUNTOS)
        envio = st.checkbox("Envío a emisario")

    with c3:
        hc = st.number_input("HC", min_value=0.0, step=0.1)
        ss = st.number_input("SS", min_value=0.0, step=0.1)
        dqo = st.number_input("DQO", min_value=0.0, step=0.1)
        sulf = st.number_input("Sulf", min_value=0.0, step=0.1)

    if st.form_submit_button("Guardar"):
        df = pd.concat([df, pd.DataFrame([{
            "datetime": datetime.combine(fecha, hora),
            "punto": punto,
            "HC": hc,
            "SS": ss,
            "DQO": dqo,
            "Sulf": sulf,
            "envio_emisario": envio
        }])], ignore_index=True)

        df.to_csv(DATA_FILE, index=False)
        st.success("Registro guardado")

# ================= EDICIÓN / BORRADO =================
st.subheader("✏️ Editar / Eliminar registros")

if not df.empty:
    df_edit = st.data_editor(
        df.sort_values("datetime", ascending=False),
        num_rows="dynamic",
        use_container_width=True
    )

    if st.button("💾 Guardar cambios"):
        df = df_edit.copy()
        df.to_csv(DATA_FILE, index=False)
        st.success("Cambios guardados")

# ================= TABLA DIARIA AGRUPADA =================
st.subheader("📊 Resumen diario de analíticas")

if not df.empty:
    df["dia"] = df["datetime"].dt.date

    df_daily = (
        df.groupby(["dia", "punto"], as_index=False)
        .agg({
            "HC": "mean",
            "SS": "mean",
            "DQO": "mean",
            "Sulf": "mean"
        })
    )

    tabla = df_daily.pivot(
        index="dia",
        columns="punto",
        values=["HC", "SS", "DQO", "Sulf"]
    )

    tabla.columns = [
        f"{param} – {punto}"
        for param, punto in tabla.columns
    ]

    def color_limites(val, parametro):
        if pd.isna(val):
            return ""
        lim = LIMITES.get(parametro)
        if not lim:
            return ""
        if val > lim["puntual"]:
            return "background-color:#ff4d4d;color:white"
        if val > lim["anual"]:
            return "background-color:#ffcc80"
        return ""

    styled = tabla.style

    for col in tabla.columns:
        param = col.split(" – ")[0]
        if param in LIMITES:
            styled = styled.applymap(
                lambda v, p=param: color_limites(v, p),
                subset=[col]
            )

    st.dataframe(
        styled,
        use_container_width=True
    )

# ================= GRÁFICOS =================
st.subheader("📈 Gráficos")

if not df.empty:
    p = st.selectbox("Punto", PUNTOS)
    param = st.selectbox("Parámetro", PARAMETROS)

    df_g = df[df["punto"] == p]

    base = alt.Chart(df_g).encode(x="datetime:T")
    linea = base.mark_line(point=True).encode(
        y=alt.Y(f"{param}:Q", title=param)
    )

    capas = [linea]

    if param in LIMITES:
        capas.append(
            alt.Chart(pd.DataFrame({"y": [LIMITES[param]["puntual"]]}))
            .mark_rule(color="red", strokeWidth=2)
            .encode(y="y:Q")
        )
        capas.append(
            alt.Chart(pd.DataFrame({"y": [LIMITES[param]["anual"]]}))
            .mark_rule(color="orange", strokeDash=[6, 4])
            .encode(y="y:Q")
        )

    st.altair_chart(
        alt.layer(*capas).properties(height=420),
        use_container_width=True
    )

# ================= PROMEDIOS =================
st.subheader("📐 Promedios acumulados (Salida FCA + Envío a emisario)")

df_p = df[
    (df["punto"] == "Salida FCA") &
    (df["envio_emisario"] == True)
].dropna(subset=["HC", "DQO"])

if df_p.empty:
    st.info("No hay datos válidos para promedio")
else:
    c1, c2 = st.columns(2)
    c1.metric("HC promedio", f"{df_p['HC'].mean():.2f} ppm")
    c2.metric("DQO promedio", f"{df_p['DQO'].mean():.2f} ppm")

# ================= PDF VISUAL =================
st.subheader("📄 Informe mensual visual (PDF)")

from io import BytesIO
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

if not df.empty:
    mes = st.selectbox(
        "Mes",
        sorted(df["datetime"].dt.to_period("M").astype(str).unique())
    )

    if st.button("📥 Generar informe visual"):
        buffer = BytesIO()

        with PdfPages(buffer) as pdf:
            df_mes = df[df["datetime"].dt.to_period("M").astype(str) == mes]
            df_mes["dia"] = df_mes["datetime"].dt.date

            for parametro in ["HC", "DQO"]:
                fig, ax = plt.subplots(figsize=(10, 5))

                for punto in ["Entrada Planta", "X-507", "Salida FCA"]:
                    df_p = (
                        df_mes[df_mes["punto"] == punto]
                        .groupby("dia")[parametro]
                        .mean()
                    )
                    ax.plot(df_p.index, df_p.values, marker="o", label=punto)

                # Límites legales
                lim = LIMITES[parametro]
                ax.axhline(lim["puntual"], color="red", linestyle="-", label="Límite puntual")
                ax.axhline(lim["anual"], color="orange", linestyle="--", label="Límite anual")

                ax.set_title(f"{parametro} – {mes}")
                ax.set_xlabel("Día")
                ax.set_ylabel("ppm")
                ax.legend()
                ax.grid(True)

                plt.tight_layout()
                pdf.savefig(fig)
                plt.close(fig)

        buffer.seek(0)

        st.download_button(
            label="⬇️ Descargar informe PDF",
            data=buffer,
            file_name=f"informe_visual_analiticas_{mes}.pdf",
            mime="application/pdf"
        )


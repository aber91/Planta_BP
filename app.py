import streamlit as st
import pandas as pd
from datetime import datetime
import os
import altair as alt
import matplotlib.pyplot as plt
from io import BytesIO
from matplotlib.backends.backend_pdf import PdfPages

# ================= CONFIG =================
DATA_FILE = "datos_analiticas.csv"
ENVIO_FILE = "envio_emisario.csv"
DATA_DIR = "data"

PUNTOS = ["Entrada Planta", "X-507", "Salida FCA"]
PARAMETROS = ["HC", "SS", "DQO", "Sulf"]

LIMITES = {
    "HC": {"puntual": 15, "anual": 2.5},
    "DQO": {"puntual": 700, "anual": 125}
}

st.set_page_config("Control Analíticas Planta", layout="wide")
st.title("💧 Control de analíticas – Planta de tratamiento de aguas")

# ================= CARGA DATOS =================
if os.path.exists(DATA_FILE):
    df = pd.read_csv(DATA_FILE, parse_dates=["datetime"])
else:
    df = pd.DataFrame(columns=["datetime", "punto", "HC", "SS", "DQO", "Sulf"])

if os.path.exists(ENVIO_FILE):
    df_envio = pd.read_csv(ENVIO_FILE, parse_dates=["dia"])
else:
    df_envio = pd.DataFrame(columns=["dia", "envio_emisario"])

# Añadimos columna día a analíticas
if not df.empty:
    df["dia"] = df["datetime"].dt.date

# ================= PESTAÑAS =================
tab_dashboard, tab_envio, tab_gestion = st.tabs(
    ["📊 Dashboard", "📅 Envío a emisario", "🛠️ Gestión de datos"]
)

# =====================================================================
# 📅 TABLA ENVÍO A EMISARIO (POR DÍA)
# =====================================================================
with tab_envio:
    st.subheader("📅 Envío a emisario (decisión diaria)")

    if df.empty:
        st.info("No hay días con datos analíticos")
    else:
        dias = (
            df[["dia"]]
            .drop_duplicates()
            .sort_values("dia")
        )

        tabla_envio = dias.merge(
            df_envio,
            on="dia",
            how="left"
        ).fillna({"envio_emisario": False})

        tabla_edit = st.data_editor(
            tabla_envio,
            column_config={
                "dia": st.column_config.DateColumn("Día"),
                "envio_emisario": st.column_config.CheckboxColumn(
                    "Envío a emisario"
                )
            },
            use_container_width=True,
            hide_index=True
        )

        if st.button("💾 Guardar decisión diaria"):
            df_envio = tabla_edit.copy()
            df_envio.to_csv(ENVIO_FILE, index=False)
            st.success("Decisiones de envío guardadas")

# =====================================================================
# 📊 DASHBOARD
# =====================================================================
with tab_dashboard:

    st.subheader("📐 Promedios acumulados (Salida FCA)")

    if df.empty or df_envio.empty:
        st.info("No hay datos suficientes para calcular promedios")
    else:
        # Días marcados como envío
        dias_envio = df_envio[
            df_envio["envio_emisario"] == True
        ]["dia"]

        df_prom = df[
            (df["punto"] == "Salida FCA") &
            (df["dia"].isin(dias_envio))
        ].dropna(subset=["HC", "DQO"])

        if df_prom.empty:
            st.info("No hay días marcados para envío a emisario")
        else:
            c1, c2 = st.columns(2)
            c1.metric("HC promedio", f"{df_prom['HC'].mean():.2f} ppm")
            c2.metric("DQO promedio", f"{df_prom['DQO'].mean():.2f} ppm")

    # ================= GRÁFICOS =================
    st.subheader("📈 Evolución de parámetros")

    col1, col2 = st.columns(2)
    punto_sel = col1.selectbox("Punto", PUNTOS)
    param_sel = col2.selectbox("Parámetro", PARAMETROS)

    df_g = df[df["punto"] == punto_sel]

    if not df_g.empty:
        base = alt.Chart(df_g).encode(x="datetime:T")

        linea = base.mark_line(point=True).encode(
            y=alt.Y(f"{param_sel}:Q", title=param_sel),
            tooltip=["datetime:T", param_sel]
        )

        capas = [linea]

        if param_sel in LIMITES:
            capas.append(
                alt.Chart(pd.DataFrame({"y": [LIMITES[param_sel]["puntual"]]}))
                .mark_rule(color="red", strokeWidth=2)
                .encode(y="y:Q")
            )
            capas.append(
                alt.Chart(pd.DataFrame({"y": [LIMITES[param_sel]["anual"]]}))
                .mark_rule(color="orange", strokeDash=[6, 4])
                .encode(y="y:Q")
            )

        chart = alt.layer(*capas).properties(height=420)
        st.altair_chart(chart, use_container_width=True)

        # -------- DESCARGAR GRÁFICO --------
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.plot(df_g["datetime"], df_g[param_sel], marker="o")

        if param_sel in LIMITES:
            ax.axhline(LIMITES[param_sel]["puntual"], color="red")
            ax.axhline(LIMITES[param_sel]["anual"], color="orange", linestyle="--")

        ax.set_title(f"{param_sel} – {punto_sel}")
        ax.grid(True)

        img = BytesIO()
        plt.tight_layout()
        plt.savefig(img, format="png")
        plt.close(fig)
        img.seek(0)

        c1, c2 = st.columns(2)

        c1.download_button(
            "⬇️ Descargar gráfico",
            data=img,
            file_name=f"{param_sel}_{punto_sel}.png",
            mime="image/png"
        )

        # -------- INFORME PDF --------
        if c2.button("📄 Generar informe"):
            buffer = BytesIO()

            with PdfPages(buffer) as pdf:
                for parametro in ["HC", "DQO"]:
                    fig, ax = plt.subplots(figsize=(10, 5))

                    for p in PUNTOS:
                        serie = (
                            df[df["punto"] == p]
                            .groupby("dia")[parametro]
                            .mean()
                        )
                        ax.plot(serie.index, serie.values, marker="o", label=p)

                    lim = LIMITES[parametro]
                    ax.axhline(lim["puntual"], color="red", label="Límite puntual")
                    ax.axhline(lim["anual"], color="orange", linestyle="--", label="Límite anual")

                    ax.set_title(f"{parametro} – Evolución diaria")
                    ax.legend()
                    ax.grid(True)

                    pdf.savefig(fig)
                    plt.close(fig)

            buffer.seek(0)

            st.download_button(
                "⬇️ Descargar informe PDF",
                data=buffer,
                file_name="informe_visual_analiticas.pdf",
                mime="application/pdf"
            )

# =====================================================================
# 🛠️ GESTIÓN DE DATOS
# =====================================================================
with tab_gestion:

    st.subheader("📥 Importar datos desde Excel")

    if st.button("Importar desde /data"):
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
                    "Sulf": r["Sulf"]
                })

        if nuevos:
            df = pd.concat([df, pd.DataFrame(nuevos)], ignore_index=True)
            df.to_csv(DATA_FILE, index=False)
            st.success(f"Importados {len(nuevos)} registros")

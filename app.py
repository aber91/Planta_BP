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
DATA_DIR = "data"

PUNTOS = ["Entrada Planta", "X-507", "Salida FCA"]
PARAMETROS = ["HC", "SS", "DQO", "Sulf"]

LIMITES = {
    "HC": {"puntual": 15, "anual": 2.5},
    "DQO": {"puntual": 700, "anual": 125}
}

st.set_page_config("Control Analíticas Planta", layout="wide")
st.title("💧 Control de analíticas – Planta de tratamiento de aguas")

# ================= CARGA =================
if os.path.exists(DATA_FILE):
    df = pd.read_csv(DATA_FILE, parse_dates=["datetime"])
else:
    df = pd.DataFrame(columns=[
        "datetime", "punto", "HC", "SS", "DQO", "Sulf", "envio_emisario"
    ])

# ================= PESTAÑAS =================
tab_dashboard, tab_gestion = st.tabs(["📊 Dashboard", "🛠️ Gestión de datos"])

# =====================================================================
# 📊 DASHBOARD
# =====================================================================
with tab_dashboard:

    # ================= PROMEDIOS =================
    st.subheader("📐 Promedios acumulados (Salida FCA + Envío a emisario)")

    df_p = df[
        (df["punto"] == "Salida FCA") &
        (df["envio_emisario"] == True)
    ].dropna(subset=["HC", "DQO"])

    if df_p.empty:
        st.info("No hay datos válidos para promedios")
    else:
        c1, c2 = st.columns(2)
        c1.metric("HC promedio", f"{df_p['HC'].mean():.2f} ppm")
        c2.metric("DQO promedio", f"{df_p['DQO'].mean():.2f} ppm")

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

        # ---------- DESCARGA IMAGEN ----------
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.plot(df_g["datetime"], df_g[param_sel], marker="o")

        if param_sel in LIMITES:
            ax.axhline(LIMITES[param_sel]["puntual"], color="red")
            ax.axhline(LIMITES[param_sel]["anual"], color="orange", linestyle="--")

        ax.set_title(f"{param_sel} – {punto_sel}")
        ax.set_ylabel("ppm")
        ax.grid(True)

        img_buffer = BytesIO()
        plt.tight_layout()
        plt.savefig(img_buffer, format="png")
        plt.close(fig)
        img_buffer.seek(0)

        c1, c2 = st.columns(2)

        with c1:
            st.download_button(
                "⬇️ Descargar gráfico",
                data=img_buffer,
                file_name=f"{param_sel}_{punto_sel}.png",
                mime="image/png"
            )

        # ---------- INFORME PDF ----------
        with c2:
            if st.button("📄 Generar informe"):
                pdf_buffer = BytesIO()

                with PdfPages(pdf_buffer) as pdf:
                    df["dia"] = df["datetime"].dt.date

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

                        ax.set_title(f"{parametro} – Evolución")
                        ax.set_ylabel("ppm")
                        ax.legend()
                        ax.grid(True)

                        plt.tight_layout()
                        pdf.savefig(fig)
                        plt.close(fig)

                pdf_buffer.seek(0)

                st.download_button(
                    "⬇️ Descargar informe PDF",
                    data=pdf_buffer,
                    file_name="informe_visual_analiticas.pdf",
                    mime="application/pdf"
                )

# =====================================================================
# 🛠️ GESTIÓN DE DATOS (OCULTO POR DEFECTO)
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

    st.subheader("✏️ Editar / eliminar registros")

    df_edit = st.data_editor(
        df.sort_values("datetime", ascending=False),
        num_rows="dynamic",
        use_container_width=True
    )

    if st.button("💾 Guardar cambios"):
        df = df_edit.copy()
        df.to_csv(DATA_FILE, index=False)
        st.success("Cambios guardados")


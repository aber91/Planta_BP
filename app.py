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

# ================= CARGA DE DATOS =================
if os.path.exists(DATA_FILE):
    df = pd.read_csv(DATA_FILE, parse_dates=["datetime"])
else:
    df = pd.DataFrame(columns=["datetime", "punto", "HC", "SS", "DQO", "Sulf"])

if not df.empty:
    df["dia"] = df["datetime"].dt.date

if os.path.exists(ENVIO_FILE):
    df_envio = pd.read_csv(ENVIO_FILE)
    df_envio["dia"] = pd.to_datetime(df_envio["dia"]).dt.date
else:
    df_envio = pd.DataFrame(columns=["dia", "envio_emisario"])

# ================= FUNCIONES =================
def analitica_valida_por_dia_salida_fca(df_in):
    """
    Para Salida FCA:
    - Si las dos últimas analíticas del día están separadas ≤ 1 minuto,
      toma el MEJOR valor (mínimo) por parámetro
    - Si no, toma la última
    """
    resultados = []

    for dia, grupo in df_in.sort_values("datetime").groupby("dia"):
        if len(grupo) == 1:
            resultados.append(grupo.iloc[-1])
            continue

        ult = grupo.iloc[-1]
        penult = grupo.iloc[-2]

        diff_min = (ult["datetime"] - penult["datetime"]).total_seconds() / 60

        if diff_min <= 1:
            fila = ult.copy()
            for p in ["HC", "SS", "DQO", "Sulf"]:
                valores = [v for v in [ult[p], penult[p]] if pd.notna(v)]
                fila[p] = min(valores) if valores else pd.NA
            resultados.append(fila)
        else:
            resultados.append(ult)

    return pd.DataFrame(resultados)

# ================= PESTAÑAS =================
tab_dashboard, tab_gestion = st.tabs(
    ["📊 Dashboard", "🛠️ Gestión de datos"]
)

# =====================================================================
# 📊 DASHBOARD
# =====================================================================
with tab_dashboard:

    # ---------- PROMEDIOS ----------
    st.subheader("📐 Promedios acumulados (Salida FCA)")

    if df.empty or df_envio.empty:
        st.info("No hay datos suficientes para calcular promedios")
    else:
        dias_envio = df_envio[df_envio["envio_emisario"]]["dia"]

        df_salida = df[
            (df["punto"] == "Salida FCA") &
            (df["dia"].isin(dias_envio))
        ]

        df_salida_valida = analitica_valida_por_dia_salida_fca(df_salida)
        df_salida_valida = df_salida_valida.dropna(subset=["HC", "DQO"])

        if df_salida_valida.empty:
            st.info("No hay datos válidos para promedio")
        else:
            c1, c2 = st.columns(2)
            c1.metric("HC promedio", f"{df_salida_valida['HC'].mean():.2f} ppm")
            c2.metric("DQO promedio", f"{df_salida_valida['DQO'].mean():.2f} ppm")

    # ---------- GRÁFICOS ----------
    st.subheader("📈 Evolución de parámetros")

    col1, col2 = st.columns(2)
    punto_sel = col1.selectbox("Punto", PUNTOS)
    param_sel = col2.selectbox("Parámetro", PARAMETROS)

    df_g = df[df["punto"] == punto_sel]

    if punto_sel == "Salida FCA":
        df_g = analitica_valida_por_dia_salida_fca(df_g)

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

        st.altair_chart(
            alt.layer(*capas).properties(height=420),
            use_container_width=True
        )

        # ---------- DESCARGA IMAGEN ----------
        df_plot = df_g.dropna(subset=[param_sel]).copy()
        if not df_plot.empty:
            fig, ax = plt.subplots(figsize=(8, 4))
            ax.plot(
                df_plot["datetime"],
                df_plot[param_sel].astype(float),
                marker="o"
            )

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

            st.download_button(
                "⬇️ Descargar gráfico",
                data=img,
                file_name=f"{param_sel}_{punto_sel}.png",
                mime="image/png"
            )

# =====================================================================
# 🛠️ GESTIÓN DE DATOS
# =====================================================================
with tab_gestion:

    # ---------- IMPORTAR ----------
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
            df["datetime"] = pd.to_datetime(df["datetime"])
            df["dia"] = df["datetime"].dt.date
            df.to_csv(DATA_FILE, index=False)
            st.success(f"Importados {len(nuevos)} registros")
        else:
            st.info("No se importaron datos")

    st.divider()

    # ---------- ENVÍO A EMISARIO ----------
    st.subheader("📅 Envío a emisario (por día)")

    if not df.empty:
        dias = df[["dia"]].drop_duplicates().sort_values("dia")

        tabla_envio = dias.merge(
            df_envio,
            on="dia",
            how="left"
        ).fillna({"envio_emisario": False})

        tabla_edit = st.data_editor(
            tabla_envio,
            column_config={
                "dia": st.column_config.DateColumn("Día"),
                "envio_emisario": st.column_config.CheckboxColumn("Envío a emisario")
            },
            hide_index=True,
            use_container_width=True
        )

        if st.button("💾 Guardar envío a emisario"):
            df_envio = tabla_edit.copy()
            df_envio.to_csv(ENVIO_FILE, index=False)
            st.success("Decisiones guardadas")
    else:
        st.info("No hay días con analíticas")

    st.divider()

    # ---------- DATOS ANALÍTICOS ----------
    st.subheader("📊 Datos analíticos")

    if not df.empty:
        df_edit = st.data_editor(
            df.drop(columns=["dia"]).sort_values("datetime", ascending=False),
            num_rows="dynamic",
            use_container_width=True
        )

        if st.button("💾 Guardar cambios en datos"):
            df = df_edit.copy()
            df["datetime"] = pd.to_datetime(df["datetime"])
            df["dia"] = df["datetime"].dt.date
            df.to_csv(DATA_FILE, index=False)
            st.success("Datos guardados")
    else:
        st.info("No hay datos cargados")

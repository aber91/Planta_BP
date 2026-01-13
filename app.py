import streamlit as st
import pandas as pd
from datetime import datetime
import os
import altair as alt
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

# ================= CONFIGURACIÓN =================
DATA_FILE = "datos_analiticas.csv"
REPORT_DIR = "reports/informes_pdf"

os.makedirs(REPORT_DIR, exist_ok=True)

PUNTOS = ["Entrada Planta", "X-507", "Salida FCA"]
PARAMETROS = ["HC", "SS", "DQO", "PH"]

LIMITES = {
    "HC": {"puntual": 15, "anual": 2.5},
    "DQO": {"puntual": 700, "anual": 125}
}

st.set_page_config(
    page_title="Control Analíticas Planta",
    layout="wide"
)

st.title("💧 Control de analíticas – Planta de tratamiento de aguas")

# ================= CARGA DE DATOS =================
if os.path.exists(DATA_FILE):
    df = pd.read_csv(DATA_FILE, parse_dates=["datetime"])
else:
    df = pd.DataFrame(columns=[
        "datetime",
        "punto",
        "HC",
        "SS",
        "DQO",
        "PH",
        "envio_emisario"
    ])

# ================= ENTRADA DE DATOS =================
st.subheader("📝 Introducción manual de datos")

with st.form("form_entrada"):
    c1, c2, c3 = st.columns(3)

    with c1:
        fecha = st.date_input("Fecha", value=datetime.today())
        hora = st.time_input("Hora", value=datetime.now().time())

    with c2:
        punto = st.selectbox("Punto de control", PUNTOS)
        envio_emisario = st.checkbox("Envío a emisario")

    with c3:
        hc = st.number_input("HC", min_value=0.0, step=0.1)
        ss = st.number_input("SS", min_value=0.0, step=0.1)
        dqo = st.number_input("DQO", min_value=0.0, step=0.1)
        ph = st.number_input("PH", min_value=0.0, step=0.1)

    guardar = st.form_submit_button("➕ Guardar analítica")

    if guardar:
        nueva_fila = {
            "datetime": datetime.combine(fecha, hora),
            "punto": punto,
            "HC": hc,
            "SS": ss,
            "DQO": dqo,
            "PH": ph,
            "envio_emisario": envio_emisario
        }
        df = pd.concat([df, pd.DataFrame([nueva_fila])], ignore_index=True)
        df.to_csv(DATA_FILE, index=False)
        st.success("Analítica guardada correctamente")

# ================= TABLA =================
st.subheader("📊 Registros de analíticas")

if df.empty:
    st.info("No hay datos registrados todavía")
else:
    st.dataframe(
        df.sort_values("datetime", ascending=False),
        use_container_width=True
    )

# ================= GRÁFICOS =================
st.subheader("📈 Visualización de datos")

if not df.empty:
    col1, col2 = st.columns(2)

    with col1:
        punto_sel = st.selectbox("Punto", PUNTOS, key="graf_punto")

    with col2:
        parametro_sel = st.selectbox("Parámetro", PARAMETROS, key="graf_param")

    df_graf = df[df["punto"] == punto_sel]

    if not df_graf.empty:
        base = alt.Chart(df_graf).encode(
            x=alt.X("datetime:T", title="Fecha y hora")
        )

        linea = base.mark_line(point=True).encode(
            y=alt.Y(f"{parametro_sel}:Q", title=parametro_sel),
            tooltip=["datetime:T", parametro_sel]
        )

        capas = [linea]

        # Límites legales
        if parametro_sel in LIMITES:
            lim = LIMITES[parametro_sel]

            # Límite puntual
            capas.append(
                alt.Chart(pd.DataFrame({"y": [lim["puntual"]]}))
                .mark_rule(color="red", strokeWidth=2)
                .encode(y="y:Q")
            )

            # Límite anual
            capas.append(
                alt.Chart(pd.DataFrame({"y": [lim["anual"]]}))
                .mark_rule(color="orange", strokeDash=[6, 4])
                .encode(y="y:Q")
            )

        grafico = alt.layer(*capas).properties(height=420)

        st.altair_chart(grafico, use_container_width=True)
    else:
        st.warning("No hay datos para el punto seleccionado")

# ================= PROMEDIOS =================
st.subheader("📐 Promedios acumulados (Salida FCA + Envío a emisario)")

df_prom = df[
    (df["punto"] == "Salida FCA") &
    (df["envio_emisario"] == True)
]

if df_prom.empty:
    st.info("No hay datos válidos para el cálculo de promedios")
else:
    col1, col2 = st.columns(2)
    col1.metric("HC promedio acumulado", f"{df_prom['HC'].mean():.2f} ppm")
    col2.metric("DQO promedio acumulado", f"{df_prom['DQO'].mean():.2f} ppm")

# ================= INFORME PDF =================
st.subheader("📄 Generación de informe mensual (PDF)")

if not df.empty:
    meses = sorted(df["datetime"].dt.to_period("M").astype(str).unique())
    mes_sel = st.selectbox("Selecciona mes", meses)

    if st.button("📥 Generar informe PDF"):
        ruta_pdf = f"{REPORT_DIR}/informe_{mes_sel}.pdf"

        c = canvas.Canvas(ruta_pdf, pagesize=A4)
        texto = c.beginText(40, 800)

        texto.textLine(f"Informe mensual de analíticas – {mes_sel}")
        texto.textLine("")
        texto.textLine("Fecha | Punto | HC | SS | DQO | PH | Envío emisario")
        texto.textLine("-" * 90)

        df_mes = df[df["datetime"].dt.to_period("M").astype(str) == mes_sel]

        for _, r in df_mes.iterrows():
            texto.textLine(
                f"{r['datetime']} | {r['punto']} | "
                f"{r['HC']} | {r['SS']} | {r['DQO']} | {r['PH']} | "
                f"{'Sí' if r['envio_emisario'] else 'No'}"
            )

        c.drawText(texto)
        c.save()

        st.success(f"Informe generado correctamente: {ruta_pdf}")

st.caption("Aplicación Streamlit – Control de analíticas de planta")

import streamlit as st
import pandas as pd
from datetime import datetime, date
import os
import altair as alt
import matplotlib.pyplot as plt
from io import BytesIO

# =====================================================
# CONFIGURACIÓN GENERAL
# =====================================================
DATA_FILE = "datos_analiticas.csv"
ENVIO_FILE = "envio_emisario.csv"
DATA_DIR = "data"

PUNTOS = ["Entrada Planta", "X-507", "Salida FCA"]
PARAMETROS = ["HC", "SS", "DQO", "Sulf"]

LIMITES = {
    "HC": {"puntual": 15, "anual": 2.5},
    "DQO": {"puntual": 700, "anual": 125}
}

st.set_page_config(page_title="Control de analíticas", layout="wide")
st.title("💧 Control de analíticas – Planta de tratamiento de aguas")

# =====================================================
# CARGA DE DATOS
# =====================================================
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

# =====================================================
# FUNCIONES DE NEGOCIO
# =====================================================
def analitica_valida_por_dia_salida_fca(df_in):
    """
    Salida FCA:
    - Una analítica al día
    - Si las dos últimas están a ≤1 minuto → mejor valor
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
                vals = [v for v in [ult[p], penult[p]] if pd.notna(v)]
                fila[p] = min(vals) if vals else pd.NA
            resultados.append(fila)
        else:
            resultados.append(ult)

    return pd.DataFrame(resultados)

def estado_global(hc, dqo):
    if pd.isna(hc) or pd.isna(dqo):
        return "⚪ Sin dato"
    if hc > LIMITES["HC"]["puntual"] or dqo > LIMITES["DQO"]["puntual"]:
        return "🔴 NO CONFORME"
    if hc > LIMITES["HC"]["anual"] or dqo > LIMITES["DQO"]["anual"]:
        return "🟠 ATENCIÓN"
    return "🟢 OK"

# =====================================================
# PESTAÑAS
# =====================================================
tab_estado, tab_dashboard, tab_gestion = st.tabs(
    ["🟢 Estado de la planta", "📊 Dashboard", "🛠️ Gestión de datos"]
)

# =====================================================
# 🟢 ESTADO DE LA PLANTA
# =====================================================
with tab_estado:

    st.subheader("🟢 HOY – Estado actual de la planta (Salida FCA)")

    hoy = date.today()

    envio_hoy = (
        not df_envio.empty and
        hoy in df_envio[df_envio["envio_emisario"]]["dia"].values
    )

    df_hoy = df[
        (df["punto"] == "Salida FCA") &
        (df["dia"] == hoy)
    ]

    if df_hoy.empty:
        st.warning("No hay analítica de hoy")
    else:
        fila = analitica_valida_por_dia_salida_fca(df_hoy).iloc[0]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("HC", fila["HC"])
        c2.metric("DQO", fila["DQO"])
        c3.metric("SS", fila["SS"])
        c4.metric("Sulf", fila["Sulf"])

        st.markdown(
            f"""
            **Última analítica válida:** {fila['datetime'].strftime('%H:%M')}  
            **Envío a emisario:** {'✅ Sí' if envio_hoy else '❌ No'}
            """
        )

        st.info(f"**Estado global:** {estado_global(fila['HC'], fila['DQO'])}")

    st.divider()

    st.subheader("📋 Parte diario de planta (Salida FCA)")

    if df.empty:
        st.info("No hay datos históricos")
    else:
        df_salida = df[df["punto"] == "Salida FCA"]
        df_valida = analitica_valida_por_dia_salida_fca(df_salida)

        parte = df_valida[["dia", "HC", "DQO"]].copy()
        parte["Envío"] = parte["dia"].isin(
            df_envio[df_envio["envio_emisario"]]["dia"]
        )
        parte["Estado"] = parte.apply(
            lambda r: estado_global(r["HC"], r["DQO"]), axis=1
        )

        st.dataframe(
            parte.sort_values("dia", ascending=False),
            use_container_width=True
        )

# =====================================================
# 📊 DASHBOARD (PROMEDIOS + GRÁFICOS)
# =====================================================
with tab_dashboard:

    st.subheader("📐 Promedios acumulados (Salida FCA)")

    if df.empty or df_envio.empty:
        st.info("No hay datos suficientes")
    else:
        dias_envio = df_envio[df_envio["envio_emisario"]]["dia"]

        df_salida = df[
            (df["punto"] == "Salida FCA") &
            (df["dia"].isin(dias_envio))
        ]

        df_valida = analitica_valida_por_dia_salida_fca(df_salida)
        df_valida = df_valida.dropna(subset=["HC", "DQO"])

        if df_valida.empty:
            st.info("No hay días válidos para promedio")
        else:
            c1, c2 = st.columns(2)
            c1.metric("HC promedio", f"{df_valida['HC'].mean():.2f} ppm")
            c2.metric("DQO promedio", f"{df_valida['DQO'].mean():.2f} ppm")

    st.subheader("📈 Evolución de parámetros")

    col1, col2 = st.columns(2)
    punto_sel = col1.selectbox("Punto", PUNTOS)
    param_sel = col2.selectbox("Parámetro", PARAMETROS)

    df_g = df[df["punto"] == punto_sel]
    if punto_sel == "Salida FCA":
        df_g = analitica_valida_por_dia_salida_fca(df_g)

    if not df_g.empty:
        chart = alt.Chart(df_g).mark_line(point=True).encode(
            x="datetime:T",
            y=alt.Y(f"{param_sel}:Q", title=param_sel),
            tooltip=["datetime:T", param_sel]
        )

        if param_sel in LIMITES:
            chart += alt.Chart(pd.DataFrame(
                {"y": [LIMITES[param_sel]["puntual"]]}
            )).mark_rule(color="red").encode(y="y:Q")

        st.altair_chart(chart, use_container_width=True)

# =====================================================
# 🛠️ GESTIÓN DE DATOS
# =====================================================
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
            df["datetime"] = pd.to_datetime(df["datetime"])
            df["dia"] = df["datetime"].dt.date
            df.to_csv(DATA_FILE, index=False)
            st.success(f"Importados {len(nuevos)} registros")

    st.divider()

    st.subheader("📅 Envío a emisario (por día)")

    if not df.empty:
        dias = df[["dia"]].drop_duplicates().sort_values("dia")
        tabla = dias.merge(df_envio, on="dia", how="left").fillna(False)

        tabla_edit = st.data_editor(
            tabla,
            hide_index=True,
            use_container_width=True
        )

        if st.button("💾 Guardar envío"):
            df_envio = tabla_edit.copy()
            df_envio.to_csv(ENVIO_FILE, index=False)
            st.success("Envío a emisario guardado")

    st.divider()

    st.subheader("📊 Datos analíticos")

    if not df.empty:
        df_edit = st.data_editor(
            df.drop(columns=["dia"]).sort_values("datetime", ascending=False),
            num_rows="dynamic",
            use_container_width=True
        )

        if st.button("💾 Guardar cambios"):
            df = df_edit.copy()
            df["datetime"] = pd.to_datetime(df["datetime"])
            df["dia"] = df["datetime"].dt.date
            df.to_csv(DATA_FILE, index=False)
            st.success("Datos actualizados")

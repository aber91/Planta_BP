# =====================================================
# app.py – v2.6 COMPLETO, ESTABLE Y UX LIMPIO
# =====================================================

import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import os
import sqlite3
import altair as alt

# =====================================================
# CONFIGURACIÓN GENERAL
# =====================================================

# Persistencia robusta (Streamlit Cloud / local)
if os.path.isdir("/mount/data"):
    PERSISTENT_DIR = "/mount/data"
else:
    PERSISTENT_DIR = "data"
    os.makedirs(PERSISTENT_DIR, exist_ok=True)

DB_PATH = os.path.join(PERSISTENT_DIR, "planta.db")

@st.cache_resource
def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

conn = get_conn()

PUNTOS = ["Entrada Planta", "X-507", "Salida FCA"]
PARAMETROS = ["HC", "SS", "DQO", "Sulf"]

LIMITES = {
    "HC": {"puntual": 15, "anual": 2.5},
    "DQO": {"puntual": 700, "anual": 125},
}

st.set_page_config(page_title="Control de analíticas", layout="wide")
st.title("💧 Control de analíticas – Planta de tratamiento de aguas")

# =====================================================
# BASE DE DATOS
# =====================================================

conn.execute("""
CREATE TABLE IF NOT EXISTS analiticas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    datetime TEXT NOT NULL,
    punto TEXT NOT NULL,
    HC REAL,
    SS REAL,
    DQO REAL,
    Sulf REAL,
    UNIQUE(datetime, punto)
)
""")

conn.execute("""
CREATE TABLE IF NOT EXISTS envio_emisario (
    dia TEXT PRIMARY KEY,
    envio_emisario INTEGER NOT NULL
)
""")

conn.commit()

# =====================================================
# CARGA DE DATOS
# =====================================================

df = pd.read_sql("SELECT * FROM analiticas", conn, parse_dates=["datetime"])
if not df.empty:
    df["dia"] = df["datetime"].dt.date
else:
    df["dia"] = []

df_envio = pd.read_sql("SELECT * FROM envio_emisario", conn)
if not df_envio.empty:
    df_envio["dia"] = pd.to_datetime(df_envio["dia"]).dt.date

# =====================================================
# FUNCIONES DE NEGOCIO
# =====================================================

def analitica_valida_salida_fca(df_in):
    resultados = []

    for dia, g in df_in.sort_values("datetime").groupby("dia"):
        if len(g) == 1:
            resultados.append(g.iloc[-1])
        else:
            ult = g.iloc[-1]
            pen = g.iloc[-2]

            if (ult["datetime"] - pen["datetime"]).total_seconds() <= 60:
                fila = ult.copy()
                for p in PARAMETROS:
                    vals = [ult[p], pen[p]]
                    vals = [v for v in vals if pd.notna(v)]
                    fila[p] = min(vals) if vals else None
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

tab_dashboard, tab_gestion = st.tabs(
    ["📊 Dashboard", "🛠️ Gestión de datos"]
)

# =====================================================
# 📊 DASHBOARD (TODO INTEGRADO AQUÍ)
# =====================================================

with tab_dashboard:

    # ---------- PROMEDIO MENSUAL ----------
    st.subheader("📐 Promedio mensual – Salida FCA")

    if df.empty:
        st.info("No hay analíticas")
    elif df_envio.empty or df_envio["envio_emisario"].sum() == 0:
        st.warning("No hay días marcados como envío a emisario")
    else:
        dias_env = df_envio[df_envio["envio_emisario"] == 1]["dia"]
        df_val = analitica_valida_salida_fca(
            df[df["punto"] == "Salida FCA"]
        )
        df_val = df_val[df_val["dia"].isin(dias_env)]

        if not df_val.empty:
            c1, c2 = st.columns(2)
            c1.metric("HC medio", f"{df_val['HC'].mean():.2f}")
            c2.metric("DQO medio", f"{df_val['DQO'].mean():.2f}")

    st.divider()

    # ---------- ESTADO HOY ----------
    st.subheader("🟢 Estado de la planta – HOY (Salida FCA)")

    hoy = date.today()
    df_hoy = df[(df["punto"] == "Salida FCA") & (df["dia"] == hoy)]

    if not df_hoy.empty:
        fila = analitica_valida_salida_fca(df_hoy).iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("HC", fila["HC"])
        c2.metric("DQO", fila["DQO"])
        c3.metric("SS", fila["SS"])
        c4.metric("Sulf", fila["Sulf"])
        st.info(f"Estado: {estado_global(fila['HC'], fila['DQO'])}")
    else:
        st.warning("No hay analítica para hoy")

    st.divider()

    # ---------- GRÁFICOS (MEJORADOS) ----------
    st.subheader("📈 Análisis gráfico")
    
    c1, c2, c3, c4 = st.columns(4)
    
    punto_sel = c1.selectbox(
        "Punto",
        ["Salida FCA", "Comparativo"],
        index=0,
        key="graf_punto"
    )
    
    param_sel = c2.selectbox(
        "Parámetro",
        PARAMETROS,
        index=PARAMETROS.index("HC"),
        key="graf_param"
    )
    
    periodo_sel = c3.selectbox(
        "Periodo",
        ["Últimos 7 días", "Últimos 30 días", "Mes actual", "Rango personalizado"],
        index=1,
        key="graf_periodo"
    )
    
    f_ini = f_fin = None
    if periodo_sel == "Rango personalizado":
        f_ini = c4.date_input("Desde", key="graf_ini")
        f_fin = c4.date_input("Hasta", key="graf_fin")
    
    # --- Filtrado por periodo ---
    df_plot = df.copy()
    now = datetime.now()
    
    if periodo_sel == "Últimos 7 días":
        df_plot = df_plot[df_plot["datetime"] >= now - timedelta(days=7)]
    elif periodo_sel == "Últimos 30 días":
        df_plot = df_plot[df_plot["datetime"] >= now - timedelta(days=30)]
    elif periodo_sel == "Mes actual":
        df_plot = df_plot[df_plot["datetime"] >= now.replace(day=1)]
    elif periodo_sel == "Rango personalizado" and f_ini and f_fin:
        df_plot = df_plot[
            (df_plot["datetime"] >= pd.to_datetime(f_ini)) &
            (df_plot["datetime"] <= pd.to_datetime(f_fin))
        ]
    
    capas = []
    
    # --- Líneas ---
    if punto_sel == "Comparativo":
        colores = {
            "Entrada Planta": "steelblue",
            "X-507": "darkorange",
            "Salida FCA": "seagreen",
        }
        for p in PUNTOS:
            df_p = df_plot[df_plot["punto"] == p]
            if p == "Salida FCA":
                df_p = analitica_valida_salida_fca(df_p)
    
            capas.append(
                alt.Chart(df_p).mark_line(point=True).encode(
                    x="datetime:T",
                    y=f"{param_sel}:Q",
                    color=alt.value(colores[p]),
                    tooltip=["datetime:T", param_sel]
                )
            )
    else:
        df_p = df_plot[df_plot["punto"] == punto_sel]
        if punto_sel == "Salida FCA":
            df_p = analitica_valida_salida_fca(df_p)
    
        capas.append(
            alt.Chart(df_p).mark_line(point=True).encode(
                x="datetime:T",
                y=f"{param_sel}:Q",
                tooltip=["datetime:T", param_sel]
            )
        )
    
    # --- Bandas de límites ---
    if param_sel in LIMITES:
        limites_df = pd.DataFrame({
            "y1": [LIMITES[param_sel]["anual"]],
            "y2": [LIMITES[param_sel]["puntual"]],
        })
    
        capas.insert(
            0,
            alt.Chart(limites_df).mark_area(
                opacity=0.2,
                color="orange"
            ).encode(
                y="y1",
                y2="y2"
            )
        )
    
    if capas:
        st.altair_chart(
            alt.layer(*capas).resolve_scale(y="shared"),
            use_container_width=True
        )
    else:
        st.info("No hay datos para el gráfico")

      # ---------- ESTADO DIARIO MENSUAL ----------
    with st.expander("📅 Estado diario de la planta (mes)"):
        df_salida = df[df["punto"] == "Salida FCA"]
        df_mes = analitica_valida_salida_fca(df_salida)

        if not df_mes.empty:
            df_mes["Estado"] = df_mes.apply(
                lambda r: estado_global(r["HC"], r["DQO"]), axis=1
            )

            st.dataframe(
                df_mes[["dia", "HC", "DQO", "Estado"]]
                .sort_values("dia", ascending=False),
                use_container_width=True,
            )
        else:
            st.info("No hay datos para el mes")

    st.divider()
    
# =====================================================
# 🛠️ GESTIÓN DE DATOS
# =====================================================

with tab_gestion:

    # ---------- INTRODUCCIÓN MANUAL ----------
    with st.expander("➕ Introducción manual de analítica"):
        c1, c2, c3 = st.columns(3)

        fecha = c1.date_input("Fecha")
        hora = c1.time_input("Hora")
        punto = c1.selectbox("Punto", PUNTOS)

        hc = c2.number_input("HC")
        ss = c2.number_input("SS")
        dqo = c3.number_input("DQO")
        sulf = c3.number_input("Sulf")

        if st.button("Guardar analítica"):
            dt = datetime.combine(fecha, hora).strftime("%Y-%m-%d %H:%M:%S")
            conn.execute(
                """
                INSERT OR REPLACE INTO analiticas
                (datetime, punto, HC, SS, DQO, Sulf)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (dt, punto, hc, ss, dqo, sulf),
            )
            conn.commit()
            st.success("Analítica guardada")

    # ---------- TABLA EDITABLE ----------
    with st.expander("📊 Tabla de analíticas"):
        if not df.empty:
            df_edit = st.data_editor(
                df.drop(columns=["dia"]),
                use_container_width=True,
                hide_index=True,
            )

            if st.button("Guardar cambios en tabla"):
                conn.execute("DELETE FROM analiticas")
                df_edit["datetime"] = pd.to_datetime(
                    df_edit["datetime"]
                ).dt.strftime("%Y-%m-%d %H:%M:%S")
                df_edit.to_sql("analiticas", conn, if_exists="append", index=False)
                conn.commit()
                st.success("Tabla actualizada")

    # ---------- ENVÍO A EMISARIO ----------
    with st.expander("📅 Envío a emisario"):
        if not df.empty:
            dias = df[["dia"]].drop_duplicates().sort_values("dia")
            tabla_env = dias.merge(
                df_envio, on="dia", how="left"
            ).fillna({"envio_emisario": 0})

            tabla_edit = st.data_editor(
                tabla_env,
                hide_index=True,
                use_container_width=True,
            )

            if st.button("Guardar envío a emisario"):
                conn.execute("DELETE FROM envio_emisario")
                tabla_edit.to_sql(
                    "envio_emisario", conn, if_exists="append", index=False
                )
                conn.commit()
                st.success("Envío a emisario actualizado")

    # ---------- IMPORTACIÓN XLSX ----------
    with st.expander("📥 Importación de datos XLSX"):
        st.info("Archivos esperados en /data")

        archivos = {
            "entrada_planta.xlsx": "Entrada Planta",
            "x507.xlsx": "X-507",
            "salidafca.xlsx": "Salida FCA",
        }

        if st.button("Importar XLSX"):
            total_insertados = 0

            for archivo, punto in archivos.items():
                ruta = os.path.join("data", archivo)
                if not os.path.exists(ruta):
                    st.warning(f"No encontrado: {archivo}")
                    continue

                df_xls = pd.read_excel(
                    ruta,
                    engine="openpyxl",
                    usecols="C:H",
                    names=["Fecha", "Hora", "HC", "SS", "DQO", "Sulf"],
                    header=None,
                    skiprows=1,
                )

                for _, r in df_xls.iterrows():
                    try:
                        dt = datetime.combine(
                            pd.to_datetime(r["Fecha"]).date(),
                            pd.to_datetime(r["Hora"]).time(),
                        )
                        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                    except Exception:
                        continue

                    conn.execute(
                        """
                        INSERT OR REPLACE INTO analiticas
                        (datetime, punto, HC, SS, DQO, Sulf)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            dt_str,
                            punto,
                            r["HC"],
                            r["SS"],
                            r["DQO"],
                            r["Sulf"],
                        ),
                    )
                    total_insertados += 1

            conn.commit()
            st.success(
                f"Importación completada: {total_insertados} registros procesados"
            )
            st.rerun()

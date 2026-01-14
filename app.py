# =====================================================
# app.py – Control de analíticas (REFactor completo)
# =====================================================

import streamlit as st
import pandas as pd
import sqlite3
import os
from datetime import datetime, date, timedelta
import calendar
import plotly.graph_objects as go

# =====================================================
# CONFIGURACIÓN GENERAL
# =====================================================
st.set_page_config(page_title="Control de analíticas", layout="wide")
st.title("💧 Control de analíticas – Planta de tratamiento de aguas")

PERSISTENT_DIR = "data"
os.makedirs(PERSISTENT_DIR, exist_ok=True)
DB_PATH = os.path.join(PERSISTENT_DIR, "planta.db")

PUNTOS = ["Entrada Planta", "X-507", "Salida FCA"]
PARAMETROS = ["HC", "SS", "DQO", "Sulf"]

LIMITES = {
    "HC": {"puntual": 15, "anual": 2.5},
    "DQO": {"puntual": 700, "anual": 125},
}

ANIO_ACTUAL = date.today().year

# =====================================================
# BASE DE DATOS
# =====================================================
@st.cache_resource
def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS analiticas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        datetime TEXT,
        punto TEXT,
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
        envio_emisario INTEGER
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS estimados_upa (
        anio INTEGER,
        parametro TEXT,
        valor REAL,
        PRIMARY KEY (anio, parametro)
    )
    """)

    conn.commit()

conn = get_conn()
init_db(conn)

# =====================================================
# CARGA DE DATOS (CACHEADA)
# =====================================================
@st.cache_data
def load_data():
    df = pd.read_sql("SELECT * FROM analiticas", conn, parse_dates=["datetime"])
    if not df.empty:
        df["dia"] = df["datetime"].dt.date
        df["mes"] = df["datetime"].dt.month
        df["anio"] = df["datetime"].dt.year

    df_envio = pd.read_sql("SELECT * FROM envio_emisario", conn)
    if not df_envio.empty:
        df_envio["dia"] = pd.to_datetime(df_envio["dia"]).dt.date

    df_est = pd.read_sql(
        "SELECT * FROM estimados_upa WHERE anio = ?",
        conn,
        params=(ANIO_ACTUAL,)
    )

    return df, df_envio, df_est

df, df_envio, df_est = load_data()

# =====================================================
# FUNCIONES DE NEGOCIO
# =====================================================
def analitica_valida_salida_fca(df_in):
    if df_in.empty:
        return df_in

    res = []
    df_in = df_in.sort_values("datetime")

    for dia, g in df_in.groupby("dia"):
        if len(g) == 1:
            res.append(g.iloc[-1])
        else:
            ult, pen = g.iloc[-1], g.iloc[-2]
            if (ult["datetime"] - pen["datetime"]).total_seconds() <= 60:
                fila = ult.copy()
                for p in PARAMETROS:
                    vals = [ult[p], pen[p]]
                    vals = [v for v in vals if pd.notna(v)]
                    fila[p] = min(vals) if vals else None
                res.append(fila)
            else:
                res.append(ult)

    return pd.DataFrame(res)

def estado_global(hc, dqo):
    if pd.isna(hc) or pd.isna(dqo):
        return "⚪ Sin dato"
    if hc > LIMITES["HC"]["puntual"] or dqo > LIMITES["DQO"]["puntual"]:
        return "🔴 NO CONFORME"
    if hc > LIMITES["HC"]["anual"] or dqo > LIMITES["DQO"]["anual"]:
        return "🟠 ATENCIÓN"
    return "🟢 OK"

def calcular_eficiencias_diarias(df, parametro):
    if parametro not in ["HC", "DQO", "SS"] or df.empty:
        return pd.DataFrame()

    rows = []
    for dia, g in df.groupby("dia"):
        fila = {"dia": dia}
        vals = {}
        for p in PUNTOS:
            df_p = g[g["punto"] == p]
            if not df_p.empty:
                vals[p] = df_p.sort_values("datetime").iloc[-1][parametro]

        def eff(cin, cout):
            if cin is None or cout is None or cin == 0:
                return None
            return (cin - cout) / cin * 100

        fila["E_Entrada_X507"] = eff(vals.get("Entrada Planta"), vals.get("X-507"))
        fila["E_X507_Salida"] = eff(vals.get("X-507"), vals.get("Salida FCA"))
        fila["E_Entrada_Salida"] = eff(vals.get("Entrada Planta"), vals.get("Salida FCA"))

        rows.append(fila)

    return pd.DataFrame(rows)

# =====================================================
# PESTAÑAS
# =====================================================
tab_dashboard, tab_gestion = st.tabs(
    ["📊 Dashboard", "🛠️ Gestión de datos"]
)

# =====================================================
# 📊 DASHBOARD
# =====================================================
with tab_dashboard:

    # ---------- PROMEDIOS + UPA ----------
    df_salida = analitica_valida_salida_fca(df[df["punto"] == "Salida FCA"])
    dias_env = df_envio[df_envio["envio_emisario"] == 1]["dia"] if not df_envio.empty else []

    df_val = df_salida[df_salida["dia"].isin(dias_env)]
    df_anual = df_val[df_val["anio"] == ANIO_ACTUAL]

    hc_acum = df_anual["HC"].mean() if not df_anual.empty else None
    dqo_acum = df_anual["DQO"].mean() if not df_anual.empty else None

    st.subheader("📊 HC y DQO acumulados")

    c1, c2 = st.columns(2)
    c1.metric("HC acumulado", f"{hc_acum:.2f}" if hc_acum else "—")
    c2.metric("DQO acumulado", f"{dqo_acum:.1f}" if dqo_acum else "—")

    st.divider()

    # ---------- ANÁLISIS GRÁFICO ----------
    st.subheader("📈 Análisis gráfico")

    c1, c2, c3 = st.columns(3)
    punto_sel = c1.selectbox("Punto", PUNTOS + ["Comparativo"], index=2)
    param_sel = c2.selectbox("Parámetro", PARAMETROS, index=0)
    periodo = c3.selectbox("Periodo", ["7 días", "30 días", "Mes actual"])

    df_plot = df.copy()
    now = datetime.now()
    if periodo == "7 días":
        df_plot = df_plot[df_plot["datetime"] >= now - timedelta(days=7)]
    elif periodo == "30 días":
        df_plot = df_plot[df_plot["datetime"] >= now - timedelta(days=30)]
    else:
        df_plot = df_plot[df_plot["datetime"] >= now.replace(day=1)]

    fig = go.Figure()

    if punto_sel == "Comparativo":
        colores = {"Entrada Planta": "blue", "X-507": "orange", "Salida FCA": "green"}
        for p in PUNTOS:
            df_p = df_plot[df_plot["punto"] == p]
            if p == "Salida FCA":
                df_p = analitica_valida_salida_fca(df_p)
            fig.add_trace(go.Scatter(
                x=df_p["datetime"], y=df_p[param_sel],
                mode="lines+markers", name=p,
                line=dict(color=colores[p])
            ))
    else:
        df_p = df_plot[df_plot["punto"] == punto_sel]
        if punto_sel == "Salida FCA":
            df_p = analitica_valida_salida_fca(df_p)
        df_p = df_p.sort_values("datetime")

        fig.add_trace(go.Scatter(
            x=df_p["datetime"], y=df_p[param_sel],
            mode="lines+markers", name=punto_sel
        ))

        df_p["EMA7"] = df_p[param_sel].ewm(span=7, adjust=False).mean()
        fig.add_trace(go.Scatter(
            x=df_p["datetime"], y=df_p["EMA7"],
            mode="lines", name="EMA 7",
            line=dict(width=3, color="orange")
        ))

    if param_sel in LIMITES:
        fig.add_hline(y=LIMITES[param_sel]["anual"], line_dash="dash", line_color="orange")
        fig.add_hline(y=LIMITES[param_sel]["puntual"], line_dash="dash", line_color="red")

    fig.update_layout(height=450, hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    # ---------- EFICIENCIAS ----------
    st.markdown(f"### 🧪 Eficiencia de eliminación – {param_sel}")
    df_eff = calcular_eficiencias_diarias(df_plot, param_sel)

    if not df_eff.empty:
        fig_e = go.Figure()
        fig_e.add_trace(go.Scatter(x=df_eff["dia"], y=df_eff["E_Entrada_Salida"],
                                   mode="lines+markers", name="Entrada → Salida FCA"))
        fig_e.add_hline(y=70, line_dash="dot", line_color="orange")
        fig_e.add_hline(y=50, line_dash="dot", line_color="red")
        fig_e.update_layout(height=300, yaxis_title="Eficiencia (%)")
        st.plotly_chart(fig_e, use_container_width=True)

# =====================================================
# 🛠️ GESTIÓN DE DATOS
# =====================================================
with tab_gestion:

    with st.expander("➕ Introducción manual"):
        fecha = st.date_input("Fecha")
        hora = st.time_input("Hora")
        punto = st.selectbox("Punto", PUNTOS)
        vals = {p: st.number_input(p) for p in PARAMETROS}

        if st.button("Guardar"):
            dt = datetime.combine(fecha, hora).strftime("%Y-%m-%d %H:%M:%S")
            conn.execute("""
                INSERT OR REPLACE INTO analiticas
                (datetime, punto, HC, SS, DQO, Sulf)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (dt, punto, vals["HC"], vals["SS"], vals["DQO"], vals["Sulf"]))
            conn.commit()
            st.cache_data.clear()
            st.rerun()

    with st.expander("📊 Tabla de analíticas"):
        st.data_editor(df.drop(columns=["dia", "mes", "anio"]), hide_index=True)

    with st.expander("💾 Copia de seguridad"):
        with open(DB_PATH, "rb") as f:
            st.download_button("Descargar BBDD", f, "planta_backup.db")
        up = st.file_uploader("Restaurar BBDD", type=["db"])
        if up:
            with open(DB_PATH, "wb") as f:
                f.write(up.read())
            st.cache_data.clear()
            st.rerun()

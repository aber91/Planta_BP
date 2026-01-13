# =====================================================
# app.py – v2.2 FULL
# Control de analíticas – Planta de tratamiento de aguas
# Persistencia: SQLite (BBDD real)
# Importación: XLSX (openpyxl)
# =====================================================

import streamlit as st
import pandas as pd
from datetime import datetime, date
import os
import sqlite3
import altair as alt

# =====================================================
# CONFIGURACIÓN GENERAL
# =====================================================
# Directorio persistente (Cloud o local)
if os.path.exists("/mount/data") and os.access("/mount/data", os.W_OK):
    PERSISTENT_DIR = "/mount/data"
else:
    PERSISTENT_DIR = "data"

os.makedirs(PERSISTENT_DIR, exist_ok=True)

DB_PATH = os.path.join(PERSISTENT_DIR, "planta.db")

# CSV legacy (solo migración inicial)
LEGACY_ANALITICAS = "datos_analiticas.csv"
LEGACY_ENVIO = "envio_emisario.csv"

PUNTOS = ["Entrada Planta", "X-507", "Salida FCA"]
PARAMETROS = ["HC", "SS", "DQO", "Sulf"]

LIMITES = {
    "HC": {"puntual": 15, "anual": 2.5},
    "DQO": {"puntual": 700, "anual": 125}
}

st.set_page_config(
    page_title="Control de analíticas – Planta",
    layout="wide"
)
st.title("💧 Control de analíticas – Planta de tratamiento de aguas")

# =====================================================
# BASE DE DATOS (SQLite)
# =====================================================
def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS analiticas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            datetime TEXT NOT NULL,
            punto TEXT NOT NULL,
            HC REAL,
            SS REAL,
            DQO REAL,
            Sulf REAL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS envio_emisario (
            dia TEXT PRIMARY KEY,
            envio_emisario INTEGER NOT NULL
        )
    """)

    # Índice único para evitar duplicados
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_analitica_unica
        ON analiticas(datetime, punto)
    """)

    conn.commit()
    conn.close()

def migrate_legacy_if_needed():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM analiticas")
    tiene_datos = cur.fetchone()[0] > 0

    if not tiene_datos and os.path.exists(LEGACY_ANALITICAS):
        df_legacy = pd.read_csv(LEGACY_ANALITICAS, parse_dates=["datetime"])
        if not df_legacy.empty:
            df_legacy["datetime"] = df_legacy["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
            df_legacy.to_sql("analiticas", conn, if_exists="append", index=False)

    if os.path.exists(LEGACY_ENVIO):
        df_env = pd.read_csv(LEGACY_ENVIO)
        if not df_env.empty:
            df_env.to_sql("envio_emisario", conn, if_exists="replace", index=False)

    conn.close()

# Inicialización
if not os.path.exists(DB_PATH):
    init_db()
    migrate_legacy_if_needed()
else:
    init_db()

# =====================================================
# CARGA DE DATOS
# =====================================================
conn = get_conn()

df = pd.read_sql(
    "SELECT * FROM analiticas",
    conn,
    parse_dates=["datetime"]
)
df["dia"] = pd.to_datetime(df["datetime"], errors="coerce").dt.date

df_envio = pd.read_sql(
    "SELECT * FROM envio_emisario",
    conn
)
df_envio["dia"] = pd.to_datetime(df_envio["dia"], errors="coerce").dt.date

# =====================================================
# FUNCIONES DE NEGOCIO
# =====================================================
def analitica_valida_por_dia_salida_fca(df_in):
    resultados = []

    for dia, grupo in df_in.sort_values("datetime").groupby("dia"):
        if len(grupo) == 1:
            resultados.append(grupo.iloc[-1])
            continue

        ult = grupo.iloc[-1]
        penult = grupo.iloc[-2]
        diff = (ult["datetime"] - penult["datetime"]).total_seconds() / 60

        if diff <= 1:
            fila = ult.copy()
            for p in PARAMETROS:
                vals = [v for v in [ult[p], penult[p]] if pd.notna(v)]
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
tab_estado, tab_dashboard, tab_gestion = st.tabs(
    ["🟢 Estado de la planta", "📊 Dashboard", "🛠️ Gestión de datos"]
)

# =====================================================
# 🟢 ESTADO DE LA PLANTA
# =====================================================
with tab_estado:
    st.subheader("🟢 HOY – Estado actual (Salida FCA)")

    hoy = date.today()

    df_hoy = df[
        (df["punto"] == "Salida FCA") &
        (df["dia"] == hoy)
    ]

    envio_hoy = hoy in df_envio[df_envio["envio_emisario"] == 1]["dia"].values

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
            **Estado global:** {estado_global(fila['HC'], fila['DQO'])}
            """
        )

    st.divider()

    st.subheader("📋 Parte diario de planta (Salida FCA)")

    df_salida = df[df["punto"] == "Salida FCA"]
    df_valida = analitica_valida_por_dia_salida_fca(df_salida)

    if not df_valida.empty:
        parte = df_valida[["dia", "HC", "DQO"]].copy()
        parte["Envío"] = parte["dia"].isin(
            df_envio[df_envio["envio_emisario"] == 1]["dia"]
        )
        parte["Estado"] = parte.apply(
            lambda r: estado_global(r["HC"], r["DQO"]), axis=1
        )

        st.dataframe(
            parte.sort_values("dia", ascending=False),
            use_container_width=True
        )

# =====================================================
# 📊 DASHBOARD
# =====================================================
with tab_dashboard:
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
        st.altair_chart(chart, use_container_width=True)

# =====================================================
# 🛠️ GESTIÓN DE DATOS
# =====================================================
with tab_gestion:

    # ---------- IMPORTAR DESDE EXCEL ----------
    st.subheader("📥 Importar datos desde Excel (.xlsx)")

    if st.button("Importar desde carpeta /data"):
        archivos = {
            "entrada_planta.xlsx": "Entrada Planta",
            "x507.xlsx": "X-507",
            "salidafca.xlsx": "Salida FCA"
        }

        insertados = 0

        for archivo, punto in archivos.items():
            ruta = os.path.join("data", archivo)
            if not os.path.exists(ruta):
                continue

            xls = pd.read_excel(
                ruta,
                engine="openpyxl",
                usecols="C:H",
                names=["Fecha", "Hora", "HC", "SS", "DQO", "Sulf"],
                header=None,
                skiprows=1
            )

            for _, r in xls.iterrows():
                try:
                    dt = datetime.combine(
                        pd.to_datetime(r["Fecha"]).date(),
                        pd.to_datetime(r["Hora"]).time()
                    )
                    dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    continue

                existe = conn.execute(
                    """
                    SELECT 1 FROM analiticas
                    WHERE datetime = ? AND punto = ?
                    LIMIT 1
                    """,
                    (dt_str, punto)
                ).fetchone()

                if existe:
                    continue

                conn.execute(
                    """
                    INSERT INTO analiticas
                    (datetime, punto, HC, SS, DQO, Sulf)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (dt_str, punto, r["HC"], r["SS"], r["DQO"], r["Sulf"])
                )
                insertados += 1

        conn.commit()
        st.success(f"Importadas {insertados} analíticas nuevas")

    st.divider()
    # ---------- TABLA DATOS ----------
    st.divider()
    st.subheader("📊 Datos analíticos (editar / borrar)")
    
    if df.empty:
        st.info("No hay analíticas cargadas")
    else:
        df_edit = df.copy()
    
        # Mostrar tabla editable
        df_editable = st.data_editor(
            df_edit.drop(columns=["dia"]),
            use_container_width=True,
            num_rows="dynamic",
            hide_index=True
        )
    
        if st.button("💾 Guardar cambios en analíticas"):
            # Normalizar datetime antes de guardar
            df_editable["datetime"] = pd.to_datetime(
                df_editable["datetime"],
                errors="coerce"
            ).dt.strftime("%Y-%m-%d %H:%M:%S")
    
            # Reemplazar tabla completa (como antes)
            conn.execute("DELETE FROM analiticas")
            df_editable.to_sql(
                "analiticas",
                conn,
                if_exists="append",
                index=False
            )
            conn.commit()
    
            st.success("Analíticas actualizadas correctamente")
            st.experimental_rerun()
        
    # ---------- AÑADIR MANUAL ----------
    st.subheader("➕ Añadir analítica manual")

    with st.form("add_manual"):
        fecha = st.date_input("Fecha")
        hora = st.time_input("Hora")
        punto = st.selectbox("Punto", PUNTOS)
        hc = st.number_input("HC", value=0.0)
        ss = st.number_input("SS", value=0.0)
        dqo = st.number_input("DQO", value=0.0)
        sulf = st.number_input("Sulf", value=0.0)

        if st.form_submit_button("Guardar"):
            dt = datetime.combine(fecha, hora)
            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")

            conn.execute(
                """
                INSERT OR IGNORE INTO analiticas
                (datetime, punto, HC, SS, DQO, Sulf)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (dt_str, punto, hc, ss, dqo, sulf)
            )
            conn.commit()
            st.success("Analítica guardada")

    st.divider()

    # ---------- ENVÍO A EMISARIO ----------
    st.subheader("📅 Envío a emisario (por día)")

    if not df.empty:
        dias = df[["dia"]].drop_duplicates().sort_values("dia")
        tabla = dias.merge(
            df_envio,
            on="dia",
            how="left"
        ).fillna({"envio_emisario": 0})

        tabla_edit = st.data_editor(
            tabla,
            hide_index=True,
            use_container_width=True
        )

        if st.button("💾 Guardar envío a emisario"):
            conn.execute("DELETE FROM envio_emisario")
            tabla_edit.to_sql("envio_emisario", conn, if_exists="append", index=False)
            conn.commit()
            st.success("Envío a emisario actualizado")

conn.close()

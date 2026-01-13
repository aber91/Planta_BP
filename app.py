# =====================================================
# app.py – v2.3 FINAL ESTABLE
# Control de analíticas – Planta de tratamiento de aguas
# =====================================================

import streamlit as st
import pandas as pd
from datetime import datetime, date
import os
import sqlite3
import altair as alt

# =====================================================
# CONFIGURACIÓN ENTORNO / PERSISTENCIA
# =====================================================
if os.getenv("STREAMLIT_CLOUD"):
    PERSISTENT_DIR = "/mount/data"
else:
    PERSISTENT_DIR = "data"

os.makedirs(PERSISTENT_DIR, exist_ok=True)
DB_PATH = os.path.join(PERSISTENT_DIR, "planta.db")

PUNTOS = ["Entrada Planta", "X-507", "Salida FCA"]
PARAMETROS = ["HC", "SS", "DQO", "Sulf"]

LIMITES = {
    "HC": {"puntual": 15, "anual": 2.5},
    "DQO": {"puntual": 700, "anual": 125}
}

st.set_page_config("Control de analíticas", layout="wide")
st.title("💧 Control de analíticas – Planta de tratamiento de aguas")

# =====================================================
# BASE DE DATOS
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
        CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_analitica
        ON analiticas(datetime, punto)
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS envio_emisario (
            dia TEXT PRIMARY KEY,
            envio_emisario INTEGER NOT NULL
        )
    """)

    conn.commit()
    conn.close()

init_db()

# =====================================================
# CARGA DE DATOS
# =====================================================
conn = get_conn()

df = pd.read_sql("SELECT * FROM analiticas", conn, parse_dates=["datetime"])
df["dia"] = pd.to_datetime(df["datetime"], errors="coerce").dt.date

df_envio = pd.read_sql("SELECT * FROM envio_emisario", conn)
df_envio["dia"] = pd.to_datetime(df_envio["dia"], errors="coerce").dt.date

# =====================================================
# FUNCIONES
# =====================================================
def analitica_valida_salida_fca(df_in):
    res = []
    for dia, g in df_in.sort_values("datetime").groupby("dia"):
        if len(g) == 1:
            res.append(g.iloc[-1])
        else:
            ult, pen = g.iloc[-1], g.iloc[-2]
            if (ult["datetime"] - pen["datetime"]).total_seconds() <= 60:
                fila = ult.copy()
                for p in PARAMETROS:
                    vals = [ult[p], pen[p]]
                    vals_validos = [v for v in vals if pd.notna(v)]
                    fila[p] = min(vals_validos) if vals_validos else None

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

# =====================================================
# TABS
# =====================================================
tab_estado, tab_dashboard, tab_gestion = st.tabs(
    ["🟢 Estado planta", "📊 Dashboard", "🛠️ Gestión datos"]
)

# =====================================================
# ESTADO PLANTA
# =====================================================
with tab_estado:
    st.subheader("Estado HOY – Salida FCA")
    hoy = date.today()

    df_hoy = df[(df["punto"] == "Salida FCA") & (df["dia"] == hoy)]

    if df_hoy.empty:
        st.warning("No hay analítica de hoy")
    else:
        fila = analitica_valida_salida_fca(df_hoy).iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("HC", fila["HC"])
        c2.metric("DQO", fila["DQO"])
        c3.metric("SS", fila["SS"])
        c4.metric("Sulf", fila["Sulf"])
        st.info(f"Estado: {estado_global(fila['HC'], fila['DQO'])}")

    st.divider()
    st.subheader("📅 Estado diario del mes (Salida FCA)")
    
    if df.empty:
        st.info("No hay datos")
    else:
        # Filtrar Salida FCA
        df_salida = df[df["punto"] == "Salida FCA"]
    
        # Obtener analítica válida por día
        df_valida = analitica_valida_salida_fca(df_salida)
    
        if df_valida.empty:
            st.info("No hay analíticas válidas")
        else:
            # Filtrar mes actual
            mes_actual = date.today().replace(day=1)
            df_mes = df_valida[df_valida["dia"] >= mes_actual]
    
            tabla_estado = pd.DataFrame({
                "Día": df_mes["dia"],
                "HC": df_mes["HC"],
                "DQO": df_mes["DQO"],
            })
    
            tabla_estado["Estado"] = tabla_estado.apply(
                lambda r: estado_global(r["HC"], r["DQO"]),
                axis=1
            )
    
            st.dataframe(
                tabla_estado.sort_values("Día", ascending=False),
                use_container_width=True
            )
    
# =====================================================
# DASHBOARD
# =====================================================
with tab_dashboard:
    st.subheader("📐 Promedio mensual – Salida FCA (con envío a emisario)")
    
    if df.empty or df_envio.empty:
        st.info("No hay datos suficientes para promedio")
    else:
        # Días con envío a emisario
        dias_envio = df_envio[df_envio["envio_emisario"] == 1]["dia"]
    
        # Salida FCA
        df_salida = df[df["punto"] == "Salida FCA"]
    
        # Analítica válida por día
        df_valida = analitica_valida_salida_fca(df_salida)
    
        # Filtrar solo días con envío
        df_env = df_valida[df_valida["dia"].isin(dias_envio)]
    
        # Filtrar mes actual
        mes_actual = date.today().replace(day=1)
        df_env_mes = df_env[df_env["dia"] >= mes_actual]
    
        if df_env_mes.empty:
            st.info("No hay días válidos este mes")
        else:
            c1, c2 = st.columns(2)
    
            c1.metric(
                "HC promedio mensual",
                f"{df_env_mes['HC'].mean():.2f} ppm"
            )
            c2.metric(
                "DQO promedio mensual",
                f"{df_env_mes['DQO'].mean():.2f} ppm"
            )

    col1, col2 = st.columns(2)
    punto_sel = col1.selectbox("Punto", PUNTOS, key="dash_punto")
    param_sel = col2.selectbox("Parámetro", PARAMETROS, key="dash_param")

    df_g = df[df["punto"] == punto_sel]
    if punto_sel == "Salida FCA":
        df_g = analitica_valida_salida_fca(df_g)

    if not df_g.empty:
        chart = alt.Chart(df_g).mark_line(point=True).encode(
            x="datetime:T",
            y=f"{param_sel}:Q"
        )
        st.altair_chart(chart, use_container_width=True)

# =====================================================
# GESTIÓN DE DATOS
# =====================================================
with tab_gestion:

    # -------- IMPORTACIÓN --------
    st.subheader("📥 Importar datos Excel (.xlsx)")

    if st.button("Importar desde /data", key="btn_import"):
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

                try:
                    conn.execute(
                        """
                        INSERT INTO analiticas
                        (datetime, punto, HC, SS, DQO, Sulf)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (dt_str, punto, r["HC"], r["SS"], r["DQO"], r["Sulf"])
                    )
                    insertados += 1
                except sqlite3.IntegrityError:
                    pass

        conn.commit()
        st.success(f"Importadas {insertados} analíticas")
        st.rerun()

    st.divider()

st.divider()
st.subheader("💾 Copia de seguridad de la base de datos")

st.divider()
st.subheader("💾 Copia de seguridad de la base de datos")

# ---------- BACKUP ----------
try:
    with open(DB_PATH, "rb") as f:
        st.download_button(
            label="📥 Descargar backup de la base de datos",
            data=f,
            file_name="planta_backup.db",
            mime="application/octet-stream",
            key="download_db_backup"
        )
except FileNotFoundError:
    st.warning("No se ha encontrado la base de datos para hacer backup")

st.markdown(
    """
    ⚠️ **Recomendación**  
    Descarga un backup:
    - antes de cerrar un mes  
    - antes de grandes cambios  
    - antes de reimportar datos  
    """
)

# ---------- RESTORE ----------
st.subheader("♻️ Restaurar base de datos")

uploaded_db = st.file_uploader(
    "Selecciona un archivo de backup (.db)",
    type=["db"],
    key="upload_db_backup"
)

if uploaded_db is not None:
    st.warning(
        "⚠️ Esta acción sobrescribirá TODOS los datos actuales. "
        "Asegúrate de que el archivo es correcto."
    )

    if st.button("🔁 Restaurar base de datos", key="restore_db_btn"):
        with open(DB_PATH, "wb") as f:
            f.write(uploaded_db.read())

        st.success("Base de datos restaurada correctamente.")
        st.info("La aplicación se reiniciará para cargar los nuevos datos.")
        st.rerun()
    
    # -------- MANUAL --------
    with st.expander("➕ Añadir analítica manual", expanded=False):
        c1, c2, c3 = st.columns(3)
        with c1:
            fecha = st.date_input("Fecha", key="man_fecha")
            hora = st.time_input("Hora", key="man_hora")
            punto = st.selectbox("Punto", PUNTOS, key="man_punto")
        with c2:
            hc = st.number_input("HC", key="man_hc")
            ss = st.number_input("SS", key="man_ss")
        with c3:
            dqo = st.number_input("DQO", key="man_dqo")
            sulf = st.number_input("Sulf", key="man_sulf")

        if st.button("Guardar analítica", key="man_guardar"):
            dt = datetime.combine(fecha, hora).strftime("%Y-%m-%d %H:%M:%S")
            try:
                conn.execute(
                    """
                    INSERT INTO analiticas
                    (datetime, punto, HC, SS, DQO, Sulf)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (dt, punto, hc, ss, dqo, sulf)
                )
                conn.commit()
                st.success("Analítica guardada")
                st.rerun()
            except sqlite3.IntegrityError:
                st.warning("Analítica ya existente")

    st.divider()

    # -------- TABLA EDITABLE --------
    st.subheader("📊 Analíticas (editar / borrar)")

    if df.empty:
        st.info("No hay datos")
    else:
        df_edit = st.data_editor(
            df.drop(columns=["dia"]),
            hide_index=True,
            use_container_width=True,
            num_rows="dynamic",
            key="editor_analiticas"
        )

        if st.button("💾 Guardar cambios", key="btn_save_table"):
            df_edit["datetime"] = pd.to_datetime(
                df_edit["datetime"], errors="coerce"
            ).dt.strftime("%Y-%m-%d %H:%M:%S")

            conn.execute("DELETE FROM analiticas")
            df_edit.to_sql("analiticas", conn, if_exists="append", index=False)
            conn.commit()
            st.success("Datos actualizados")
            st.rerun()

    st.divider()

    # -------- ENVÍO EMISARIO --------
    st.subheader("📅 Envío a emisario (por día)")

    if not df.empty:
        dias = df[["dia"]].drop_duplicates().sort_values("dia")
        tabla = dias.merge(df_envio, on="dia", how="left").fillna({"envio_emisario": 0})

        tabla_edit = st.data_editor(
            tabla,
            hide_index=True,
            use_container_width=True,
            key="editor_envio"
        )

        if st.button("Guardar envío", key="btn_envio"):
            conn.execute("DELETE FROM envio_emisario")
            tabla_edit.to_sql("envio_emisario", conn, if_exists="append", index=False)
            conn.commit()
            st.success("Envío actualizado")
            st.rerun()

conn.close()

import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import os

import plotly.graph_objects as go
import calendar
import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool

# =====================================================
# SESSION STATE – CONTROL DE CARGAS
# =====================================================

if "df" not in st.session_state:
    st.session_state.df = None

if "df_envio" not in st.session_state:
    st.session_state.df_envio = None

if "df_est" not in st.session_state:
    st.session_state.df_est = None

if "df_caudal" not in st.session_state:
    st.session_state.df_caudal = None

# =====================================================
# CONFIGURACIÓN GENERAL Y PERSISTENCIA (SUPABASE)
# =====================================================

def _get_db_settings():
    required_keys = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]
    db_settings = {}
    missing = []

    for key in required_keys:
        value = st.secrets.get(key) if key in st.secrets else os.getenv(key)
        if value in (None, ""):
            missing.append(key)
        else:
            db_settings[key] = value

    sslmode = st.secrets.get("DB_SSLMODE") if "DB_SSLMODE" in st.secrets else os.getenv("DB_SSLMODE", "require")
    db_settings["DB_SSLMODE"] = sslmode

    if missing:
        raise RuntimeError(
            "Faltan credenciales de base de datos: " + ", ".join(missing) +
            ". Configúralas en Streamlit secrets o variables de entorno."
        )

    return db_settings


@st.cache_resource
def get_pool():
    db = _get_db_settings()
    return SimpleConnectionPool(
        minconn=1,
        maxconn=5,
        host=db["DB_HOST"],
        port=db["DB_PORT"],
        dbname=db["DB_NAME"],
        user=db["DB_USER"],
        password=db["DB_PASSWORD"],
        sslmode=db["DB_SSLMODE"],
        cursor_factory=psycopg2.extras.RealDictCursor,
    )

def get_conn():
    return get_pool().getconn()

def put_conn(conn):
    if conn is not None:
        get_pool().putconn(conn)

def ejecutar_sql(sql, params=None):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    finally:
        put_conn(conn)

def ejecutar_sql_many(sql, params_list):
    if not params_list:
        return
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, params_list)
        conn.commit()
    finally:
        put_conn(conn)

# -----------------------------------------------------
# 🔌 CHECK CONEXIÓN BASE DE DATOS (NEON)
# -----------------------------------------------------
def check_db_connection():
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            cur.fetchone()
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        if conn is not None:
            put_conn(conn)

ok_db, db_error = check_db_connection()

st.sidebar.markdown("### 🗄️ Estado base de datos")

if ok_db:
    st.sidebar.success("🟢 Conectado correctamente a Neon")
else:
    st.sidebar.error("🔴 Error de conexión a Neon")
    st.sidebar.code(db_error)
    st.error("No se pudo iniciar la app porque faltan o son inválidas las credenciales de base de datos.")
    st.stop()

# =====================================================
# RUTA ÚNICA DE BASE DE DATOS SQLITE
# =====================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PERSISTENT_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(PERSISTENT_DIR, exist_ok=True)

DB_PATH = os.path.join(PERSISTENT_DIR, "planta.db")

# -----------------------------------------------------
# CONSTANTES DE NEGOCIO
# -----------------------------------------------------
PUNTOS_BP = ["Entrada Planta", "X-507", "Salida FCA"]
PUNTOS = PUNTOS_BP + ["Pluviales"]
PARAMETROS = ["HC", "SS", "DQO", "Sulf"]

LIMITES = {
    "HC": {"puntual": 15, "anual": 2.5},
    "DQO": {"puntual": 700, "anual": 125},
}

LIMITES_PLUVIALES = {
    "HC": 5,
    "DQO": 125,
}

anio = date.today().year

# -----------------------------------------------------
# CONFIGURACIÓN STREAMLIT
# -----------------------------------------------------
st.set_page_config(page_title="Control de analíticas", layout="wide")
st.title("💧 Control de analíticas – Planta de tratamiento de aguas")

# =====================================================
# BASE DE DATOS – ESTRUCTURA (CREACIÓN ÚNICA Y SEGURA)
# =====================================================

# ⚠️ IMPORTANTE:
# - Este bloque se ejecuta UNA SOLA VEZ al arranque
# - NO volver a crear tablas en otras partes de la app
# - NO usar conn.execute ni conn.commit aquí

@st.cache_resource
def init_db():
    ejecutar_sql("""
    CREATE TABLE IF NOT EXISTS analiticas (
        id BIGSERIAL PRIMARY KEY,
        ts TIMESTAMP NOT NULL,
        punto TEXT NOT NULL,
        HC DOUBLE PRECISION,
        SS DOUBLE PRECISION,
        DQO DOUBLE PRECISION,
        Sulf DOUBLE PRECISION,
        UNIQUE (ts, punto)
    );
    """)

    ejecutar_sql("""
    CREATE TABLE IF NOT EXISTS envio_emisario (
        dia DATE PRIMARY KEY,
        envio_emisario INTEGER NOT NULL CHECK (envio_emisario IN (0,1))
    );
    """)

    ejecutar_sql("""
    CREATE TABLE IF NOT EXISTS caudal_emisario (
        ts TIMESTAMP PRIMARY KEY,
        caudal_m3h DOUBLE PRECISION NOT NULL
    );
    """)

    ejecutar_sql("""
    CREATE TABLE IF NOT EXISTS estimados_upa (
        anio INTEGER NOT NULL,
        parametro TEXT NOT NULL,
        valor DOUBLE PRECISION NOT NULL,
        PRIMARY KEY (anio, parametro)
    );
    """)

init_db()

# =====================================================
# CARGA DE DATOS DESDE NEON (CONTROLADA POR SESIÓN)
# =====================================================

def cargar_tabla(query, params=None):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
        return pd.DataFrame(rows)
    finally:
        put_conn(conn)

def cargar_analiticas():
    df_tmp = cargar_tabla(
        """
        SELECT
            id,
            ts,
            punto,
            hc,
            ss,
            dqo,
            sulf
        FROM analiticas
        ORDER BY ts
        """
    )

    if not df_tmp.empty:
        df_tmp["ts"] = pd.to_datetime(df_tmp["ts"], errors="coerce")
        df_tmp = df_tmp.dropna(subset=["ts"])
        df_tmp["dia"] = df_tmp["ts"].dt.date

        df_tmp = df_tmp.rename(
            columns={
                "hc": "HC",
                "ss": "SS",
                "dqo": "DQO",
                "sulf": "Sulf",
            }
        )
    else:
        df_tmp = pd.DataFrame(
            columns=["id", "ts", "punto", "HC", "SS", "DQO", "Sulf", "dia"]
        )

    return df_tmp

def cargar_envio_emisario():
    df_envio_tmp = cargar_tabla("""
        SELECT
            dia,
            envio_emisario
        FROM envio_emisario
    """)

    if not df_envio_tmp.empty:
        df_envio_tmp["dia"] = pd.to_datetime(
            df_envio_tmp["dia"],
            errors="coerce"
        ).dt.date
        df_envio_tmp = df_envio_tmp.dropna(subset=["dia"])
    else:
        df_envio_tmp = pd.DataFrame(
            columns=["dia", "envio_emisario"]
        )

    return df_envio_tmp


def cargar_caudal_emisario():
    df_caudal_tmp = cargar_tabla("""
        SELECT
            ts,
            caudal_m3h
        FROM caudal_emisario
        ORDER BY ts
    """)

    if not df_caudal_tmp.empty:
        df_caudal_tmp["ts"] = pd.to_datetime(df_caudal_tmp["ts"], errors="coerce")
        df_caudal_tmp = df_caudal_tmp.dropna(subset=["ts"])
        df_caudal_tmp["dia"] = df_caudal_tmp["ts"].dt.date
    else:
        df_caudal_tmp = pd.DataFrame(columns=["ts", "caudal_m3h", "dia"])

    return df_caudal_tmp

def cargar_estimados(anio_actual):
    return cargar_tabla(
        """
        SELECT
            anio,
            parametro,
            valor
        FROM estimados_upa
        WHERE anio = %s
        """,
        (anio_actual,)
    )

def recargar_datos(recargar_analiticas=True, recargar_envio=True, recargar_estimados=True, recargar_caudal=True):
    if recargar_analiticas:
        st.session_state.df = cargar_analiticas()
    if recargar_envio:
        st.session_state.df_envio = cargar_envio_emisario()
    if recargar_estimados:
        st.session_state.df_est = cargar_estimados(anio)
    if recargar_caudal:
        st.session_state.df_caudal = cargar_caudal_emisario()


# ---------- ANALÍTICAS ----------
def cargar_datos_iniciales():
    if st.session_state.df is None:
        st.session_state.df = cargar_analiticas()
    if st.session_state.df_envio is None:
        st.session_state.df_envio = cargar_envio_emisario()
    if st.session_state.df_est is None:
        st.session_state.df_est = cargar_estimados(anio)
    if st.session_state.df_caudal is None:
        st.session_state.df_caudal = cargar_caudal_emisario()


cargar_datos_iniciales()

df = st.session_state.df.copy()
df_envio = st.session_state.df_envio.copy()
df_caudal = st.session_state.df_caudal.copy()

# -----------------------------------------------------
# ESTIMADOS UPA PERSISTENTES
# -----------------------------------------------------
df_est = st.session_state.df_est

def get_estimado(param):
    fila = df_est[df_est["parametro"] == param]
    if not fila.empty:
        return float(fila.iloc[0]["valor"])
    return None

# =====================================================
# FUNCIONES DE NEGOCIO
# =====================================================

def asegurar_ts(df):
    """
    Garantiza que el DataFrame tiene una columna 'ts'
    para ordenación y gráficos.
    """
    if "ts" in df.columns:
        return df

    df = df.copy()

    if "dia" in df.columns:
        df["ts"] = pd.to_ts(df["dia"])
        return df

    # Último recurso: no se puede ordenar
    return df
    
@st.cache_data(show_spinner=False)
def analitica_valida_salida_fca(df_in):
    resultados = []

    for dia, g in df_in.sort_values("ts").groupby("dia"):
        if len(g) == 1:
            resultados.append(g.iloc[-1])
        else:
            ult = g.iloc[-1]
            pen = g.iloc[-2]

            if (ult["ts"] - pen["ts"]).total_seconds() <= 60:
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

def estado_global_pluviales(hc, dqo):
    if pd.isna(hc) or pd.isna(dqo):
        return "⚪ Sin dato"
    if hc > LIMITES_PLUVIALES["HC"] or dqo > LIMITES_PLUVIALES["DQO"]:
        return "🔴 NO CONFORME"
    if hc > LIMITES_PLUVIALES["HC"] * 0.8 or dqo > LIMITES_PLUVIALES["DQO"] * 0.8:
        return "🟠 ATENCIÓN"
    return "🟢 OK"

def semaforo_promedio(valor, limite_anual):
    """
    Devuelve un semáforo en función de lo desviado que esté
    el valor respecto al límite anual.
    """
    if valor is None or pd.isna(valor):
        return "⚪"
    ratio = valor / limite_anual
    if ratio > 1:
        return "🔴"
    if ratio > 0.8:
        return "🟠"
    return "🟢"

def calcular_upa(valor_actual_medio, n_dias_actuales, estimado, n_dias_restantes):
    """
    Calcula la UPA (Última Previsión Anual).

    valor_actual_medio : promedio real hasta hoy
    n_dias_actuales    : nº de días con envío a emisario ya transcurridos
    estimado           : valor medio estimado hasta final de año
    n_dias_restantes   : nº de días restantes del año
    """
    if (
        valor_actual_medio is None
        or pd.isna(valor_actual_medio)
        or n_dias_actuales == 0
    ):
        return None

    total_acumulado = (
        (valor_actual_medio * n_dias_actuales)
        + (estimado * n_dias_restantes)
    )

    return total_acumulado / (n_dias_actuales + n_dias_restantes)



def formatear_numero(valor, decimales=2):
    if valor is None or pd.isna(valor):
        return "—"
    s = f"{float(valor):,.{decimales}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")

def formatear_entero(valor):
    if valor is None or pd.isna(valor):
        return "—"
    s = f"{int(round(float(valor))):,}"
    return s.replace(",", ".")

def valor_con_semaforo(valor, unidad, limite_anual):
    if valor is None or pd.isna(valor):
        return "—"
    sem = semaforo_promedio(valor, limite_anual)
    return f"{formatear_numero(valor, 2)} {unidad} {sem}"

def texto_margen(margen):
    if margen is None:
        return ""
    if margen < 0:
        return f":red[Margen previsto: {formatear_numero(margen, 1)} ppm]"
    if margen < 0.2 * abs(margen):
        return f":orange[Margen previsto: +{formatear_numero(margen, 1)} ppm]"
    return f":green[Margen previsto: +{formatear_numero(margen, 1)} ppm]"


def normalizar_filas_analiticas(df_src, punto_forzado=None):
    filas = []
    errores = 0

    for _, row in df_src.iterrows():
        try:
            ts = pd.to_datetime(row.get("ts"), errors="coerce")
            if pd.isna(ts):
                continue

            punto = punto_forzado if punto_forzado is not None else row.get("punto")
            if pd.isna(punto) or punto is None:
                continue

            def to_num(v):
                if v is None or pd.isna(v):
                    return None
                return float(v)

            filas.append((
                ts.to_pydatetime(),
                str(punto),
                to_num(row.get("HC")),
                to_num(row.get("SS")),
                to_num(row.get("DQO")),
                to_num(row.get("Sulf")),
            ))
        except Exception:
            errores += 1

    return filas, errores


def ultima_muestra_para_estado(df_punto, columnas=("HC", "DQO")):
    if df_punto.empty:
        return None

    df_ord = df_punto.sort_values("ts")
    df_util = df_ord.dropna(subset=list(columnas), how="all")
    if not df_util.empty:
        return df_util.iloc[-1]
    return df_ord.iloc[-1]


@st.cache_data(show_spinner=False)
def calcular_eficiencias_diarias(df, parametro):
    """
    Calcula eficiencias diarias por etapa:
    - Entrada -> X-507
    - X-507 -> Salida FCA
    - Entrada -> Salida FCA
    Usa la última analítica válida del día
    """
    resultados = []

    for dia, g in df.groupby("dia"):
        fila = {"dia": dia}

        valores = {}
        for punto in PUNTOS_BP:
            df_p = g[g["punto"] == punto]
            if not df_p.empty:
                valores[punto] = df_p.sort_values("ts").iloc[-1][parametro]

        def eficiencia(cin, cout):
            if cin is None or cout is None or cin == 0:
                return None
            return (cin - cout) / cin * 100

        fila["E_Entrada_X507"] = eficiencia(
            valores.get("Entrada Planta"),
            valores.get("X-507")
        )
        fila["E_X507_Salida"] = eficiencia(
            valores.get("X-507"),
            valores.get("Salida FCA")
        )
        fila["E_Entrada_Salida"] = eficiencia(
            valores.get("Entrada Planta"),
            valores.get("Salida FCA")
        )

        resultados.append(fila)

    return pd.DataFrame(resultados)

@st.cache_data(show_spinner=False)
def diagnostico_filtros_fca(df_plot, parametro):
    """
    Diagnóstico automático de filtros FCA
    Basado en EMA(7) y eficiencia X-507 → Salida FCA
    """

    resultado = {
        "estado": "🟢 Normal",
        "mensaje": "Funcionamiento dentro de parámetros normales.",
        "motivos": []
    }

    if parametro not in ["HC", "DQO"]:
        return resultado

    # --- Salida FCA ---
    df_salida = df_plot[df_plot["punto"] == "Salida FCA"].copy()
    if df_salida.empty or len(df_salida) < 7:
        return resultado

    df_salida = analitica_valida_salida_fca(df_salida)
    df_salida = df_salida.sort_values("ts")

    # --- EMA 7 ---
    df_salida["EMA7"] = df_salida[parametro].ewm(span=7, adjust=False).mean()

    # Pendiente de EMA (últimos valores)
    ema_diff = df_salida["EMA7"].diff().dropna()
    subidas = (ema_diff > 0).sum()

    # --- Eficiencia ---
    df_eff = calcular_eficiencias_diarias(df_plot, parametro)
    if df_eff.empty:
        return resultado

    eff_media = df_eff["E_X507_Salida"].dropna().tail(5).mean()

    # --- Entrada estable ---
    df_ent = df_plot[df_plot["punto"] == "Entrada Planta"]
    entrada_estable = True
    if not df_ent.empty:
        ent_vals = df_ent.sort_values("ts")[parametro].tail(5)
        if ent_vals.max() - ent_vals.min() > ent_vals.mean() * 0.2:
            entrada_estable = False

    # ------------------ LÓGICA ------------------
    if subidas >= 5 and eff_media is not None and eff_media < 60 and entrada_estable:
        resultado["estado"] = "🔴 Posible limpieza de filtros"
        resultado["mensaje"] = "Se detecta saturación progresiva del carbón activo."
        resultado["motivos"] = [
            "Tendencia creciente sostenida en Salida FCA (EMA 7)",
            "Caída de eficiencia en etapa X-507 → Salida FCA",
            "Entrada estable"
        ]

    elif subidas >= 3 or (eff_media is not None and eff_media < 70):
        resultado["estado"] = "🟠 Vigilancia"
        resultado["mensaje"] = "Se observan señales tempranas de empeoramiento."
        resultado["motivos"] = [
            "Tendencia ascendente reciente",
            "Eficiencia en descenso"
        ]

    return resultado
# =====================================================
# PESTAÑAS
# =====================================================

tab_dashboard, tab_gestion = st.tabs(
    ["📊 Dashboard", "🛠️ Gestión de datos"]
)

# =====================================================x
# 📊 DASHBOARD (TODO INTEGRADO AQUÍ)
# =====================================================

with tab_dashboard:

# =====================================================
# 📊 DASHBOARD – CONTROL + UPA + EVOLUCIÓN ANUAL
# =====================================================

    st.subheader("📊 Dashboard – Control anual de la planta")

    # -------------------------------------------------
    # Preparación de datos base (Salida FCA)
    # -------------------------------------------------
    df_salida = df[df["punto"] == "Salida FCA"].copy()

    if df_salida.empty or df_envio.empty:
        st.info("No hay datos suficientes para mostrar el dashboard.")
    else:
        df_val = analitica_valida_salida_fca(df_salida)

        dias_envio = df_envio[df_envio["envio_emisario"] == 1]["dia"]
        df_val = df_val[df_val["dia"].isin(dias_envio)]

        if df_val.empty:
            st.warning("No hay analíticas válidas con envío a emisario.")
        else:
            hoy = date.today()
            anio = hoy.year
            mes_actual = hoy.month

            # =================================================
            # LAYOUT PRINCIPAL (IZQ + DERECHA)
            # =================================================
            col_left, col_graph = st.columns([1, 1])

            # =================================================
            # 🟦 COLUMNA IZQUIERDA (2 SUBCOLUMNAS)
            # =================================================
            with col_left:
                col_acc, col_upa = st.columns([1, 1])

                # =============================================
                # 📐 ACUMULADOS
                # =============================================
                with col_acc:
                    st.markdown("### 📐 Acumulados")
                    st.caption("Salida FCA · días con envío a emisario")

                    df_anual = df_val[df_val["dia"].apply(lambda d: d.year) == anio]
                    df_mes = df_anual[df_anual["dia"].apply(lambda d: d.month) == mes_actual]

                    hc_mes = df_mes["HC"].mean() if not df_mes.empty else None
                    dqo_mes = df_mes["DQO"].mean() if not df_mes.empty else None
                    hc_anual = df_anual["HC"].mean() if not df_anual.empty else None
                    dqo_anual = df_anual["DQO"].mean() if not df_anual.empty else None

                    st.markdown("**HC (ppm)**")
                    st.metric("Mes actual", valor_con_semaforo(hc_mes, "ppm", LIMITES["HC"]["anual"]))
                    st.metric("Año acumulado", valor_con_semaforo(hc_anual, "ppm", LIMITES["HC"]["anual"]))

                    st.markdown("**DQO (ppm)**")
                    st.metric("Mes actual", valor_con_semaforo(dqo_mes, "ppm", LIMITES["DQO"]["anual"]))
                    st.metric("Año acumulado", valor_con_semaforo(dqo_anual, "ppm", LIMITES["DQO"]["anual"]))

                    df_caudal_anual = df_caudal[df_caudal["dia"].apply(lambda d: d.year) == anio] if not df_caudal.empty else pd.DataFrame()
                    df_caudal_mes = df_caudal_anual[df_caudal_anual["dia"].apply(lambda d: d.month) == mes_actual] if not df_caudal_anual.empty else pd.DataFrame()

                    caudal_mes = df_caudal_mes["caudal_m3h"].sum() if not df_caudal_mes.empty else 0.0
                    caudal_anual = df_caudal_anual["caudal_m3h"].sum() if not df_caudal_anual.empty else 0.0

                    st.markdown("**Agua enviada a emisario (m³)**")
                    st.metric("Mes actual", f"{formatear_entero(caudal_mes)} m³")
                    st.metric("Año acumulado", f"{formatear_entero(caudal_anual)} m³")

                    dqo_t_mes = (dqo_mes * caudal_mes / 1_000_000) if dqo_mes is not None else None
                    dqo_t_anual = (dqo_anual * caudal_anual / 1_000_000) if dqo_anual is not None else None

                    st.markdown("**DQO enviada (t)**")
                    st.metric("Mes actual", f"{formatear_entero(dqo_t_mes)} t")
                    st.metric("Año acumulado", f"{formatear_entero(dqo_t_anual)} t")

                # =============================================
                # 🔮 UPA
                # =============================================
                with col_upa:
                    st.markdown("### 🔮 UPA")
                    st.caption("Última previsión anual · valores persistentes")
                
                    # -------------------------------------------------
                    # Días reales considerados
                    # -------------------------------------------------
                    dias_transcurridos = len(df_anual)
                    dias_totales = 365
                    dias_restantes = max(dias_totales - dias_transcurridos, 0)
                    upa_hc = None
                    upa_dqo = None
                    est_hc_eff = None
                    est_dqo_eff = None
                
                    if dias_transcurridos == 0:
                        st.info("No hay suficientes datos para calcular la UPA.")
                    else:
                        # -------------------------------------------------
                        # Cargar estimados persistentes (BBDD)
                        # -------------------------------------------------
                        est_hc_guardado = get_estimado("HC")
                        est_dqo_guardado = get_estimado("DQO")
                
                        if est_hc_guardado is None:
                            est_hc_guardado = float(hc_anual) if hc_anual else 0.0
                
                        if est_dqo_guardado is None:
                            est_dqo_guardado = float(dqo_anual) if dqo_anual else 0.0
                
                        # -------------------------------------------------
                        # Inputs editables (NO guardan automáticamente)
                        # -------------------------------------------------
                        est_hc = st.number_input(
                            "Estimado HC medio hasta fin de año (ppm)",
                            min_value=0.0,
                            value=float(est_hc_guardado),
                            step=0.1,
                            key="upa_est_hc"
                        )
                
                        est_dqo = st.number_input(
                            "Estimado DQO medio hasta fin de año (ppm)",
                            min_value=0.0,
                            value=float(est_dqo_guardado),
                            step=1.0,
                            key="upa_est_dqo"
                        )
                
                        # -------------------------------------------------
                        # 💾 Guardar estimados UPA (Postgres)
                        # -------------------------------------------------
                        if st.button("💾 Guardar estimados UPA"):
                            ejecutar_sql_many(
                                """
                                INSERT INTO estimados_upa (anio, parametro, valor)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (anio, parametro)
                                DO UPDATE SET valor = EXCLUDED.valor
                                """,
                                [
                                    (anio, "HC", float(est_hc)),
                                    (anio, "DQO", float(est_dqo)),
                                ],
                            )
                
                            st.success("✅ Estimados UPA guardados correctamente")
                            recargar_datos(
                                recargar_analiticas=False,
                                recargar_envio=False,
                                recargar_estimados=True,
                            )
                            st.rerun()
                
                        # -------------------------------------------------
                        # 🔢 CÁLCULO ÚNICO DE UPA (FUENTE DE VERDAD)
                        # -------------------------------------------------
                        est_hc_eff = float(est_hc)
                        est_dqo_eff = float(est_dqo)
                
                        upa_hc = calcular_upa(
                            hc_anual,
                            dias_transcurridos,
                            est_hc_eff,
                            dias_restantes
                        )
                
                        upa_dqo = calcular_upa(
                            dqo_anual,
                            dias_transcurridos,
                            est_dqo_eff,
                            dias_restantes
                        )
                
                        # -------------------------------------------------
                        # Márgenes respecto al límite anual
                        # -------------------------------------------------
                        margen_hc = (
                            LIMITES["HC"]["anual"] - upa_hc
                            if upa_hc is not None else None
                        )
                
                        margen_dqo = (
                            LIMITES["DQO"]["anual"] - upa_dqo
                            if upa_dqo is not None else None
                        )
                
                        # -------------------------------------------------
                        # Salida visual
                        # -------------------------------------------------
                        st.metric(
                            "UPA HC (ppm)",
                            valor_con_semaforo(
                                upa_hc,
                                "ppm",
                                LIMITES["HC"]["anual"]
                            )
                        )
                        if margen_hc is not None:
                            st.markdown(texto_margen(margen_hc))
                
                        st.metric(
                            "UPA DQO (ppm)",
                            valor_con_semaforo(
                                upa_dqo,
                                "ppm",
                                LIMITES["DQO"]["anual"]
                            )
                        )
                        if margen_dqo is not None:
                            st.markdown(texto_margen(margen_dqo))


            # =================================================
            # 🟩 COLUMNA DERECHA – GRÁFICO ANUAL
            # =================================================
            with col_graph:
                st.markdown("### 📈 Evolución anual")

                parametro = st.selectbox(
                    "Parámetro",
                    ["HC", "DQO", "m3 enviados", "DQO enviada (t)"],
                    key="graf_anual_param"
                )

                limite = None
                if parametro in LIMITES:
                    limite = LIMITES[parametro]["anual"]

                df_val["mes"] = df_val["dia"].apply(lambda d: d.month)
                df_caudal_calc = df_caudal.copy() if not df_caudal.empty else pd.DataFrame(columns=["dia", "caudal_m3h"])
                if not df_caudal_calc.empty:
                    df_caudal_calc["dia"] = pd.to_datetime(df_caudal_calc["dia"])
                    df_caudal_calc["mes"] = df_caudal_calc["dia"].dt.month

                if parametro == "m3 enviados":
                    stats_mensual = df_caudal_calc.groupby("mes")["caudal_m3h"].sum().reindex(range(1,13)).to_frame("mean")
                    stats_mensual["count"] = 1
                    stats_mensual["sum"] = stats_mensual["mean"]
                elif parametro == "DQO enviada (t)":
                    dqo_mes_df = df_val.groupby("mes")["DQO"].mean().reindex(range(1,13))
                    m3_mes_df = df_caudal_calc.groupby("mes")["caudal_m3h"].sum().reindex(range(1,13)) if not df_caudal_calc.empty else pd.Series(index=range(1,13), dtype=float)
                    dqo_t_mes_df = (dqo_mes_df * m3_mes_df) / 1_000_000
                    stats_mensual = dqo_t_mes_df.to_frame("mean")
                    stats_mensual["count"] = 1
                    stats_mensual["sum"] = stats_mensual["mean"]
                else:
                    stats_mensual = (
                        df_val.groupby("mes")[parametro]
                        .agg(["mean", "count", "sum"])
                        .reindex(range(1, 13))
                    )

                prom_mensual = stats_mensual["mean"]
                conteos = stats_mensual["count"]
                sumas = stats_mensual["sum"]

                prom_acum = sumas.cumsum() / conteos.cumsum()

                # --------- PROYECCIÓN UPA (CON TENDENCIA MENSUAL) ---------
                proy = prom_acum.copy()

                est_eff = est_hc_eff if parametro == "HC" else est_dqo_eff
                meses_reales = conteos.dropna()

                if not meses_reales.empty and est_eff is not None:
                    ultimo_mes = meses_reales.index.max()
                    hoy = date.today()
                    anio_actual = hoy.year
                    mes_actual = hoy.month

                    suma_acum = 0.0
                    conteo_acum = 0
                    for m in range(1, 13):
                        suma_real = sumas.loc[m] if pd.notna(sumas.loc[m]) else 0.0
                        conteo_real = int(conteos.loc[m]) if pd.notna(conteos.loc[m]) else 0
                        dias_mes = calendar.monthrange(anio_actual, m)[1]

                        if m < mes_actual:
                            suma_acum += suma_real
                            conteo_acum += conteo_real
                        elif m == mes_actual:
                            dias_restantes_mes = max(dias_mes - conteo_real, 0)
                            suma_acum += suma_real + (dias_restantes_mes * est_eff)
                            conteo_acum += conteo_real + dias_restantes_mes
                        else:
                            suma_acum += dias_mes * est_eff
                            conteo_acum += dias_mes

                        if m >= ultimo_mes and conteo_acum > 0:
                            proy.loc[m] = suma_acum / conteo_acum
                
                meses = list(range(1, 13))
                nombres_meses = [calendar.month_abbr[m] for m in meses]
                
                fig = go.Figure()
                
                fig.add_bar(
                    x=nombres_meses,
                    y=prom_mensual,
                    name="Promedio mensual",
                    marker_color="#4C78A8"
                )
                
                fig.add_trace(go.Scatter(
                    x=nombres_meses,
                    y=prom_acum,
                    mode="lines+markers",
                    name="Promedio acumulado",
                    line=dict(width=3)
                ))
                
                fig.add_trace(go.Scatter(
                    x=nombres_meses,
                    y=proy,
                    mode="lines",
                    name="Proyección UPA",
                    line=dict(width=3, dash="dash")
                ))
                
                if limite is not None:
                    fig.add_hline(
                        y=limite,
                        line_dash="dot",
                        line_color="red",
                        annotation_text="Límite anual",
                        annotation_position="top left"
                    )
                
                fig.update_layout(
                    height=520,
                    margin=dict(l=20, r=20, t=40, b=20),
                    yaxis_title="ppm" if parametro in ["HC", "DQO"] else ("m³" if parametro == "m3 enviados" else "t"),
                    legend=dict(orientation="h", y=-0.25)
                )
                
                st.plotly_chart(fig, use_container_width=True)

    # ---------- ESTADO PLANTA ----------

    st.subheader("🟢 Estado actual de la planta – Último análisis disponible")
    
    df_salida = df[df["punto"] == "Salida FCA"]
    
    if df_salida.empty:
        st.warning("No hay analíticas registradas todavía.")
    else:
        # Aplicar lógica de analítica válida por día
        df_val = analitica_valida_salida_fca(df_salida)
        df_val = df_val.sort_values("ts")
        df_pluviales = df[df["punto"] == "Pluviales"].sort_values("ts")
    
        ultima = df_val.iloc[-1]
        ultima_pluv = ultima_muestra_para_estado(df_pluviales, ("HC", "DQO"))
    
        fecha_txt = ultima["ts"].strftime("%d/%m/%Y %H:%M")
        fecha_txt_pluv = (
            ultima_pluv["ts"].strftime("%d/%m/%Y %H:%M")
            if ultima_pluv is not None else "Sin dato"
        )
    
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("HC (Salida FCA)", ultima["HC"])
        c2.metric("DQO (Salida FCA)", ultima["DQO"])
        c3.metric(
            "HC (Pluviales)",
            ultima_pluv["HC"] if ultima_pluv is not None else "—"
        )
        c4.metric(
            "DQO (Pluviales)",
            ultima_pluv["DQO"] if ultima_pluv is not None else "—"
        )
    
        st.caption(f"📅 Último análisis disponible: **{fecha_txt}**")
        st.caption(f"🌧️ Último análisis pluviales: **{fecha_txt_pluv}**")
    
        estado = estado_global(ultima["HC"], ultima["DQO"])
    
        if estado.startswith("🔴"):
            st.error(f"Estado global: {estado}")
        elif estado.startswith("🟠"):
            st.warning(f"Estado global: {estado}")
        else:
            st.success(f"Estado global: {estado}")

        if ultima_pluv is not None:
            estado_pluv = estado_global_pluviales(
                ultima_pluv["HC"],
                ultima_pluv["DQO"],
            )
            if estado_pluv.startswith("🔴"):
                st.error(f"Estado global pluviales: {estado_pluv}")
            elif estado_pluv.startswith("🟠"):
                st.warning(f"Estado global pluviales: {estado_pluv}")
            else:
                st.success(f"Estado global pluviales: {estado_pluv}")
        else:
            st.info("Estado global pluviales: ⚪ Sin dato")

    # ---------- GRÁFICOS (MEJORADOS) ----------
    st.subheader("📈 Análisis gráfico")
    @st.cache_data(show_spinner=False)
    def filtrar_df_por_periodo(df, periodo_sel, f_ini, f_fin):
        now = datetime.now()
    
        if periodo_sel == "Últimos 7 días":
            return df[df["ts"] >= now - timedelta(days=7)]
    
        elif periodo_sel == "Últimos 30 días":
            return df[df["ts"] >= now - timedelta(days=30)]
    
        elif periodo_sel == "Mes actual":
            return df[df["ts"] >= now.replace(day=1)]
    
        elif periodo_sel == "Rango personalizado" and f_ini and f_fin:
            return df[
                (df["ts"] >= pd.to_datetime(f_ini)) &
                (df["ts"] <= pd.to_datetime(f_fin))
            ]
    
        return df
    # -------------------------------------------------
    # Selectores
    # -------------------------------------------------
    c1, c2, c3, c4 = st.columns(4)
    
    punto_sel = c1.selectbox(
        "Punto",
        ["Entrada Planta", "X-507", "Salida FCA", "Pluviales", "Comparativo"],
        index=2,  # Salida FCA por defecto
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
    
    # -------------------------------------------------
    # Filtrado temporal
    # -------------------------------------------------
    df_plot = filtrar_df_por_periodo(
        st.session_state.df,
        periodo_sel,
        f_ini,
        f_fin
    )
    
    # -------------------------------------------------
    # Checkbox EMA (solo sentido en no comparativo)
    # -------------------------------------------------
    mostrar_ema = False
    if punto_sel != "Comparativo":
        mostrar_ema = st.checkbox(
            "Mostrar tendencia EMA (7 analíticas)",
            value=True,
            key=f"ema7_{punto_sel}_{param_sel}"
        )
    
    # -------------------------------------------------
    # Gráfico
    # -------------------------------------------------
    if not df_plot.empty:
    
        fig = go.Figure()
    
        # =============================================
        # COMPARATIVO
        # =============================================
        if punto_sel == "Comparativo":
            colores = {
                "Entrada Planta": "blue",
                "X-507": "orange",
                "Salida FCA": "green",
                "Pluviales": "purple",
            }
    
            for p in PUNTOS:
                df_p = df_plot[df_plot["punto"] == p].copy()
                if df_p.empty:
                    continue
    
                if p == "Salida FCA":
                    df_p = analitica_valida_salida_fca(df_p)
    
                fig.add_trace(
                    go.Scatter(
                        x=df_p["ts"],
                        y=df_p[param_sel],
                        mode="lines+markers",
                        name=p,
                        line=dict(color=colores[p])
                    )
                )
    
        # =============================================
        # PUNTO ÚNICO
        # =============================================

        else:
            df_p = df_plot[df_plot["punto"] == punto_sel].copy()
        
            if punto_sel == "Salida FCA":
                df_p = analitica_valida_salida_fca(df_p)
        
            # 🔧 Asegurar columna ts para gráficos
            df_p = asegurar_ts(df_p)
        
            if "ts" in df_p.columns:
                df_p = df_p.sort_values("ts")
            else:
                st.warning("⚠️ No se puede ordenar: falta columna ts")   

            if not df_p.empty and "ts" in df_p.columns:
            
                fig.add_trace(
                    go.Scatter(
                        x=df_p["ts"],
                        y=df_p[param_sel],
                        mode="lines+markers",
                        name=punto_sel
                    )
                )
            
            else:
                st.info("ℹ️ No hay datos temporales suficientes para mostrar la tendencia.")
    
            # ---------- EMA 7 analíticas ----------
            if mostrar_ema and not df_p.empty:
                df_p["EMA7"] = (
                    df_p[param_sel]
                    .ewm(span=7, adjust=False)
                    .mean()
                )
    
                fig.add_trace(
                    go.Scatter(
                        x=df_p["ts"],
                        y=df_p["EMA7"],
                        mode="lines",
                        name="EMA 7",
                        line=dict(color="#F58518", width=3)
                    )
                )
    
        # -------------------------------------------------
        # Límites legales
        # -------------------------------------------------
        if punto_sel == "Pluviales" and param_sel in LIMITES_PLUVIALES:
            fig.add_hline(
                y=LIMITES_PLUVIALES[param_sel],
                line_dash="dash",
                line_color="red",
                annotation_text="Límite pluviales",
                annotation_position="top left"
            )
        elif param_sel in LIMITES:
            fig.add_hline(
                y=LIMITES[param_sel]["anual"],
                line_dash="dash",
                line_color="orange",
                annotation_text="Límite anual",
                annotation_position="top left"
            )
    
            fig.add_hline(
                y=LIMITES[param_sel]["puntual"],
                line_dash="dash",
                line_color="red",
                annotation_text="Límite puntual",
                annotation_position="top left"
            )
    
        # -------------------------------------------------
        # Layout
        # -------------------------------------------------
        fig.update_layout(
            height=450,
            margin=dict(l=40, r=40, t=40, b=40),
            xaxis_title="Fecha",
            yaxis_title=param_sel,
            legend_title="Punto",
            hovermode="x unified"
        )
    
        st.plotly_chart(fig, use_container_width=True)

        if punto_sel != "Comparativo" and punto_sel == "Salida FCA" and not df_caudal.empty:
            st.markdown("#### 🚚 m³ enviados (control diario)")
            df_c = df_caudal.copy()
            df_c["dia"] = pd.to_datetime(df_c["dia"])
            if "dia" in df_plot.columns:
                dias_filtrados = pd.to_datetime(df_plot["dia"].dropna().unique())
                df_c = df_c[df_c["dia"].isin(dias_filtrados)]
            df_c_dia = df_c.groupby("dia")["caudal_m3h"].sum().reset_index()
            if not df_c_dia.empty:
                fig_m3 = go.Figure()
                fig_m3.add_trace(go.Bar(x=df_c_dia["dia"], y=df_c_dia["caudal_m3h"], name="m³ enviados"))
                fig_m3.update_layout(height=260, margin=dict(l=40, r=40, t=20, b=20), xaxis_title="Fecha", yaxis_title="m³")
                st.plotly_chart(fig_m3, use_container_width=True)

    else:
        st.info("No hay datos para el gráfico")

    # -------------------------------------------------
    # Eficiencias
    # -------------------------------------------------
    
    st.markdown(f"### 🧪 Eficiencia de eliminación diaria – {param_sel}")
    st.caption(f"Porcentaje de eliminación de {param_sel} · cálculo diario")

    df_eff = calcular_eficiencias_diarias(df_plot, param_sel)
    
    if not df_eff.empty:
        df_eff = df_eff.sort_values("dia")
    
        fig_eff = go.Figure()
    
        # ---- Selectores de etapas ----
        c_eff1, c_eff2, c_eff3 = st.columns(3)
    
        mostrar_e1 = c_eff1.checkbox(
            "Entrada → X-507",
            value=False,
            key=f"eff_e1_{param_sel}"
        )
    
        mostrar_e2 = c_eff2.checkbox(
            "X-507 → Salida FCA",
            value=True,
            key=f"eff_e2_{param_sel}"
        )
    
        mostrar_e3 = c_eff3.checkbox(
            "Entrada → Salida FCA",
            value=True,
            key=f"eff_e3_{param_sel}"
        )
    
        # ---- Series ----
        if mostrar_e1:
            fig_eff.add_trace(go.Scatter(
                x=df_eff["dia"],
                y=df_eff["E_Entrada_X507"],
                mode="lines+markers",
                name="Entrada → X-507"
            ))
    
        if mostrar_e2:
            fig_eff.add_trace(go.Scatter(
                x=df_eff["dia"],
                y=df_eff["E_X507_Salida"],
                mode="lines+markers",
                name="X-507 → Salida FCA"
            ))
    
        if mostrar_e3:
            fig_eff.add_trace(go.Scatter(
                x=df_eff["dia"],
                y=df_eff["E_Entrada_Salida"],
                mode="lines+markers",
                name="Entrada → Salida FCA"
            ))
    
        # ---- Líneas de referencia ----
        fig_eff.add_hline(
            y=70,
            line_dash="dot",
            line_color="orange",
            annotation_text="Atención (70%)",
            annotation_position="top left"
        )
    
        fig_eff.add_hline(
            y=50,
            line_dash="dot",
            line_color="red",
            annotation_text="Crítico (50%)",
            annotation_position="top left"
        )
    
        fig_eff.update_layout(
            height=350,
            yaxis_title="Eficiencia (%)",
            xaxis_title="Fecha",
            yaxis=dict(range=[-20, 100]),
            legend=dict(orientation="h", y=-0.25),
            hovermode="x unified",
            margin=dict(l=40, r=40, t=40, b=40),
        )
    
        st.plotly_chart(fig_eff, use_container_width=True)
    
    else:
        st.info("No hay datos suficientes para calcular eficiencias.")

    # -------------------------------------------------
    # 🧠 MOSTRAR DIAGNÓSTICO AUTOMÁTICO
    # -------------------------------------------------
    st.markdown("### 🧠 Diagnóstico automático – HC y DQO")
    
    # -------------------------------------------------
    # 🧠 MOSTRAR RESULTADO DEL DIAGNÓSTICO
    # -------------------------------------------------
    diag_hc = diagnostico_filtros_fca(df_plot, "HC")
    diag_dqo = diagnostico_filtros_fca(df_plot, "DQO")

    col_diag_hc, col_diag_dqo = st.columns(2)

    with col_diag_hc:
        st.markdown("**HC**")
        if diag_hc is None:
            st.info("No hay datos suficientes para el diagnóstico automático.")
        else:
            if diag_hc["estado"].startswith("🔴"):
                st.error(f"{diag_hc['estado']}\n\n{diag_hc['mensaje']}")
            elif diag_hc["estado"].startswith("🟠"):
                st.warning(f"{diag_hc['estado']}\n\n{diag_hc['mensaje']}")
            else:
                st.success(f"{diag_hc['estado']}\n\n{diag_hc['mensaje']}")

            if diag_hc.get("motivos"):
                st.markdown("**Motivos detectados:**")
                for m in diag_hc["motivos"]:
                    st.markdown(f"- {m}")

    with col_diag_dqo:
        st.markdown("**DQO**")
        if diag_dqo is None:
            st.info("No hay datos suficientes para el diagnóstico automático.")
        else:
            if diag_dqo["estado"].startswith("🔴"):
                st.error(f"{diag_dqo['estado']}\n\n{diag_dqo['mensaje']}")
            elif diag_dqo["estado"].startswith("🟠"):
                st.warning(f"{diag_dqo['estado']}\n\n{diag_dqo['mensaje']}")
            else:
                st.success(f"{diag_dqo['estado']}\n\n{diag_dqo['mensaje']}")

            if diag_dqo.get("motivos"):
                st.markdown("**Motivos detectados:**")
                for m in diag_dqo["motivos"]:
                    st.markdown(f"- {m}")

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
    
        if st.button("💾 Guardar analítica"):
            ts = datetime.combine(fecha, hora)
        
            ejecutar_sql(
                """
                INSERT INTO analiticas (ts, punto, hc, ss, dqo, sulf)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    ts,
                    punto,
                    hc if hc != 0 else None,
                    ss if ss != 0 else None,
                    dqo if dqo != 0 else None,
                    sulf if sulf != 0 else None,
                )
            )
        
            st.success("Analítica guardada correctamente")
            recargar_datos(
                recargar_analiticas=True,
                recargar_envio=True,
                recargar_estimados=False,
            )
            st.rerun()
            ()
        
    # ---------- TABLA EDITABLE ----------
    with st.expander("📊 Tabla de analíticas"):
        df_bp = df[df["punto"].isin(PUNTOS_BP)].copy()
        df_pluv_tab = df[df["punto"] == "Pluviales"].copy()

        st.markdown("#### Planta BP")
        if not df_bp.empty:
            df_edit_bp = st.data_editor(
                df_bp.drop(columns=["dia"]),
                use_container_width=True,
                hide_index=True,
                key="tabla_bp_editor",
            )

            if st.button("Guardar Planta BP"):
                ejecutar_sql(
                    "DELETE FROM analiticas WHERE punto <> 'Pluviales'"
                )

                filas_bp = [
                    (
                        row["ts"],
                        row["punto"],
                        row["HC"],
                        row["SS"],
                        row["DQO"],
                        row["Sulf"],
                    )
                    for _, row in df_edit_bp.iterrows()
                ]
                ejecutar_sql_many(
                    """
                    INSERT INTO analiticas
                    (ts, punto, HC, SS, DQO, Sulf)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    filas_bp,
                )

                st.success("Tabla Planta BP actualizada correctamente")
                recargar_datos(
                    recargar_analiticas=True,
                    recargar_envio=True,
                    recargar_estimados=False,
                )
                st.rerun()
        else:
            st.info("No hay datos de Planta BP en la tabla.")

        st.markdown("#### Pluviales")
        if not df_pluv_tab.empty:
            df_edit_pluv = st.data_editor(
                df_pluv_tab.drop(columns=["dia"]),
                use_container_width=True,
                hide_index=True,
                key="tabla_pluviales_editor",
            )
        else:
            df_edit_pluv = st.data_editor(
                pd.DataFrame(columns=["id", "ts", "punto", "HC", "SS", "DQO", "Sulf"]),
                use_container_width=True,
                hide_index=True,
                key="tabla_pluviales_editor",
            )

        if st.button("Guardar Pluviales"):
            ejecutar_sql(
                "DELETE FROM analiticas WHERE punto = 'Pluviales'"
            )

            filas_pluv, errores_pluv = normalizar_filas_analiticas(
                df_edit_pluv,
                punto_forzado="Pluviales",
            )
            ejecutar_sql_many(
                """
                INSERT INTO analiticas
                (ts, punto, HC, SS, DQO, Sulf)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                filas_pluv,
            )

            st.success(f"Tabla Pluviales actualizada correctamente ({len(filas_pluv)} filas)")
            if errores_pluv > 0:
                st.warning(f"⚠️ Filas de pluviales con error: {errores_pluv}")
            recargar_datos(
                recargar_analiticas=True,
                recargar_envio=False,
                recargar_estimados=False,
            )
            st.rerun()
    
    
    # ---------- ENVÍO A EMISARIO ----------
    with st.expander("📅 Envío a emisario"):
        df_bp = df[df["punto"].isin(PUNTOS_BP)]
        if not df_bp.empty:
            dias = df_bp[["dia"]].drop_duplicates().sort_values("dia")
    
            tabla_env = dias.merge(
                df_envio, on="dia", how="left"
            ).fillna({"envio_emisario": 0})
    
            tabla_edit = st.data_editor(
                tabla_env,
                hide_index=True,
                use_container_width=True,
            )
    
            if st.button("💾 Guardar envío a emisario"):
                ejecutar_sql("DELETE FROM envio_emisario")
            
                filas_envio = [
                    (
                        r["dia"],
                        int(bool(r["envio_emisario"])),
                    )
                    for _, r in tabla_edit.iterrows()
                ]
                ejecutar_sql_many(
                    """
                    INSERT INTO envio_emisario (dia, envio_emisario)
                    VALUES (%s, %s)
                    """,
                    filas_envio,
                )
            
                st.success("Envío a emisario actualizado")
                recargar_datos(
                    recargar_analiticas=False,
                    recargar_envio=True,
                    recargar_estimados=False,
                )
                st.rerun()
                ()
            
    # ---------- COPIA DE SEGURIDAD BBDD ----------
    with st.expander("💾 Copia de seguridad y persistencia de datos"):
    
        st.markdown(
            """
            ### 🔐 BBDD (Neon PostgresSQL)
    
            ✔️ Los datos se almacenan en **Neon PostgreSQL**  
            ✔️ **No se pierden al reiniciar la app**  
            ✔️ Copias de seguridad gestionadas por Neon  
            ✔️ Exportación / importación manual mediante .db o .xlsx
            """
        )
    
        st.divider()

        # =====================================================
        # 📥 IMPORTACIÓN DE DATOS DESDE EXCEL (XLSX → NEON)
        # =====================================================

        with st.expander("📥 Importar analíticas desde Excel"):
        
            st.markdown(
                """
                **Formato esperado de los archivos XLSX**
        
                - Fila 1: encabezados  
                - Columna C → Fecha  
                - Columna E → HC  
                - Columna F → SS  
                - Columna G → DQO  
                - Columna H → Sulf  
        
                ✔️ Se permiten valores vacíos  
                ✔️ Hora asumida automáticamente (12:00)  
                ✔️ Inserción directa en Neon  
                """
            )
        
            archivos = {
                "Entrada Planta": st.file_uploader(
                    "📄 entrada_planta.xlsx", type=["xlsx"], key="xlsx_entrada"
                ),
                "X-507": st.file_uploader(
                    "📄 x507.xlsx", type=["xlsx"], key="xlsx_x507"
                ),
                "Salida FCA": st.file_uploader(
                    "📄 salidafca.xlsx", type=["xlsx"], key="xlsx_fca"
                ),
                "Pluviales": st.file_uploader(
                    "📄 pluviales.xlsx", type=["xlsx"], key="xlsx_pluviales"
                ),
            }
        
            if st.button("🚀 Importar datos XLSX"):
                total_insertados = 0
                errores = 0
                filas_insert = []
        
                for punto, archivo in archivos.items():
                    if archivo is None:
                        continue
        
                    try:
                        df_xls = pd.read_excel(
                            archivo,
                            engine="openpyxl",
                            usecols="C,E,F,G,H",
                            header=0,
                        )
        
                        df_xls.columns = ["Fecha", "HC", "SS", "DQO", "Sulf"]
        
                    except Exception as e:
                        st.error(f"❌ Error leyendo {archivo.name}: {e}")
                        continue
        
                    for _, r in df_xls.iterrows():
                        try:
                            if pd.isna(r["Fecha"]):
                                continue
        
                            ts = datetime.combine(
                                pd.to_datetime(r["Fecha"]).date(),
                                datetime.strptime("12:00", "%H:%M").time()
                            )
        
                            filas_insert.append(
                                (
                                    ts,
                                    punto,
                                    None if pd.isna(r["HC"]) else float(r["HC"]),
                                    None if pd.isna(r["SS"]) else float(r["SS"]),
                                    None if pd.isna(r["DQO"]) else float(r["DQO"]),
                                    None if pd.isna(r["Sulf"]) else float(r["Sulf"]),
                                )
                            )
                            total_insertados += 1
        
                        except Exception:
                            errores += 1

                ejecutar_sql_many(
                    """
                    INSERT INTO analiticas (ts, punto, hc, ss, dqo, sulf)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ts, punto)
                    DO UPDATE SET
                        hc = EXCLUDED.hc,
                        ss = EXCLUDED.ss,
                        dqo = EXCLUDED.dqo,
                        sulf = EXCLUDED.sulf
                    """,
                    filas_insert,
                )
        
                if total_insertados > 0:
                    st.success(f"✅ Importación completada: {total_insertados} filas insertadas")
                else:
                    st.warning("⚠️ No se insertaron filas")
        
                if errores > 0:
                    st.warning(f"⚠️ Filas con error: {errores}")
        
                ()
                if total_insertados > 0:
                    recargar_datos(
                        recargar_analiticas=True,
                        recargar_envio=True,
                        recargar_estimados=False,
                    )
                    st.rerun()


        # =====================================================
        # 📥 IMPORTAR CAUDAL A EMISARIO DESDE EXCEL
        # =====================================================
        with st.expander("📥 Importar caudal enviado a emisario (horario)"):

            st.markdown(
                """
                **Formato esperado del archivo XLS/XLSX**

                - Columna A → Fecha y hora  
                - Columna B → Caudal (m³/h)  

                Regla para actualizar `envio_emisario` por día:  
                - Se considera envío cuando caudal >= 20 m³/h  
                - Si horas con envío > 4 => 1  
                - Si horas con envío <= 4 => 0
                """
            )

            archivo_caudal = st.file_uploader(
                "📄 caudal_emisario.xls / .xlsx",
                type=["xls", "xlsx"],
                key="xlsx_caudal_emisario"
            )

            if st.button("🚀 Importar caudal enviado a emisario"):
                if archivo_caudal is None:
                    st.warning("⚠️ Sube primero un archivo XLS/XLSX")
                else:
                    filas_caudal = []
                    errores = 0

                    try:
                        df_caudal_xls = pd.read_excel(archivo_caudal, usecols="A,B", header=0)
                        df_caudal_xls.columns = ["ts", "caudal_m3h"]
                    except Exception as e:
                        st.error(f"❌ Error leyendo el archivo: {e}")
                    else:
                        # Normalización vectorizada (más rápida y robusta para ficheros horarios grandes)
                        total_filas = len(df_caudal_xls)

                        df_caudal_xls["ts"] = pd.to_datetime(
                            df_caudal_xls["ts"],
                            errors="coerce"
                        )
                        df_caudal_xls["caudal_m3h"] = pd.to_numeric(
                            df_caudal_xls["caudal_m3h"],
                            errors="coerce"
                        )

                        df_caudal_xls = df_caudal_xls.dropna(subset=["ts", "caudal_m3h"])
                        errores = total_filas - len(df_caudal_xls)

                        if not df_caudal_xls.empty:
                            df_caudal_xls = df_caudal_xls.sort_values("ts")
                            filas_caudal = list(
                                zip(
                                    df_caudal_xls["ts"].dt.to_pydatetime(),
                                    df_caudal_xls["caudal_m3h"].astype(float)
                                )
                            )

                        ejecutar_sql_many(
                            """
                            INSERT INTO caudal_emisario (ts, caudal_m3h)
                            VALUES (%s, %s)
                            ON CONFLICT (ts)
                            DO UPDATE SET caudal_m3h = EXCLUDED.caudal_m3h
                            """,
                            filas_caudal,
                        )

                        df_tmp = pd.DataFrame(filas_caudal, columns=["ts", "caudal_m3h"])
                        if not df_tmp.empty:
                            df_tmp["dia"] = pd.to_datetime(df_tmp["ts"]).dt.date
                            df_day = df_tmp.groupby("dia")["caudal_m3h"].apply(lambda x: int((x >= 20).sum() > 4)).reset_index(name="envio_emisario")
                            filas_envio_auto = [(r["dia"], int(r["envio_emisario"])) for _, r in df_day.iterrows()]
                            ejecutar_sql_many(
                                """
                                INSERT INTO envio_emisario (dia, envio_emisario)
                                VALUES (%s, %s)
                                ON CONFLICT (dia)
                                DO UPDATE SET envio_emisario = EXCLUDED.envio_emisario
                                """,
                                filas_envio_auto,
                            )

                        st.success(f"✅ Caudal importado: {len(filas_caudal)} horas")
                        if errores > 0:
                            st.warning(f"⚠️ Filas con error: {errores}")

                        recargar_datos(
                            recargar_analiticas=False,
                            recargar_envio=True,
                            recargar_estimados=False,
                            recargar_caudal=True,
                        )
                        st.rerun()

        # =====================================================
        # 📥 IMPORTAR ENVÍO A EMISARIO DESDE EXCEL
        # =====================================================
        with st.expander("📥 Importar envío a emisario desde Excel"):
        
            st.markdown(
                """
                **Formato esperado del archivo XLSX**
        
                - Columna A → Fecha  
                - Columna B → Envío a emisario  
                  - 1 / 0  
                  - Sí / No  
                  - TRUE / FALSE  
        
                ✔️ Se sobrescriben los valores del mismo día  
                ✔️ Inserción directa en Neon  
                """
            )
        
            archivo_envio = st.file_uploader(
                "📄 envio_emisario.xlsx",
                type=["xlsx"],
                key="xlsx_envio_emisario"
            )
        
            if st.button("🚀 Importar envío a emisario"):
                if archivo_envio is None:
                    st.warning("⚠️ Sube primero un archivo XLSX")
                else:
                    insertados = 0
                    errores = 0
                    filas_envio = []
        
                    try:
                        df_env = pd.read_excel(
                            archivo_envio,
                            engine="openpyxl",
                            usecols="A,B",
                            header=0,
                        )
        
                        df_env.columns = ["dia", "envio_emisario"]
        
                    except Exception as e:
                        st.error(f"❌ Error leyendo el archivo: {e}")
                    else:
                        for _, r in df_env.iterrows():
                            try:
                                if pd.isna(r["dia"]):
                                    continue
        
                                dia = pd.to_datetime(r["dia"]).date()
        
                                val = r["envio_emisario"]
        
                                # Normalizar valores
                                if isinstance(val, str):
                                    val = val.strip().lower()
                                    envio = 1 if val in ["1", "si", "sí", "true", "yes"] else 0
                                else:
                                    envio = 1 if int(val) == 1 else 0
        
                                filas_envio.append((dia, envio))
                                insertados += 1
        
                            except Exception:
                                errores += 1

                        ejecutar_sql_many(
                            """
                            INSERT INTO envio_emisario (dia, envio_emisario)
                            VALUES (%s, %s)
                            ON CONFLICT (dia)
                            DO UPDATE SET envio_emisario = EXCLUDED.envio_emisario
                            """,
                            filas_envio,
                        )
        
                        if insertados > 0:
                            st.success(f"✅ Envío a emisario importado: {insertados} días")
                        else:
                            st.warning("⚠️ No se insertaron filas")
        
                        if errores > 0:
                            st.warning(f"⚠️ Filas con error: {errores}")
        
                        ()
                        if insertados > 0:
                            recargar_datos(
                                recargar_analiticas=False,
                                recargar_envio=True,
                                recargar_estimados=False,
                            )
                            st.rerun()
        
        # -------------------------------------------------
        # 📤 EXPORTAR DATOS (CSV)
        # -------------------------------------------------
        with st.expander("💾 Exportar base de datos (.db)"):
        
            if os.path.exists(DB_PATH):
                with open(DB_PATH, "rb") as f:
                    st.download_button(
                        "⬇️ Descargar base de datos actual",
                        data=f,
                        file_name="planta.db",
                        mime="application/octet-stream"
                    )
            else:
                st.warning("No existe ninguna base de datos para exportar.")

    
        # ---------- IMPORTAR / RESTAURAR BASE DE DATOS ----------
        with st.expander("📤 Restaurar base de datos (.db)"):
        
            uploaded_db = st.file_uploader(
                "Selecciona un archivo .db",
                type=["db"]
            )
        
            if uploaded_db is not None:
                st.warning("⚠️ Esta acción sobrescribirá TODOS los datos actuales.")
        
                if st.button("🔁 Restaurar base de datos"):
                    try:
                        # Cerrar conexiones activas
                        try:
                            conn = get_conn()
                            conn.close()
                        except Exception:
                            pass
        
                        # Sobrescribir la DB
                        with open(DB_PATH, "wb") as f:
                            f.write(uploaded_db.read())
        
                        st.success("✅ Base de datos restaurada correctamente.")
                        st.info("🔄 Recargando aplicación…")
        
                        ()
        
                    except Exception as e:
                        st.error(f"❌ Error restaurando la base de datos: {e}")


st.sidebar.markdown("### 🧪 Diagnóstico DB")
st.sidebar.write("Existe DB:", os.path.exists(DB_PATH))

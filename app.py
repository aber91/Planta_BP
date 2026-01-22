# =====================================================
# CONFIGURACIÓN GENERAL Y PERSISTENCIA (SUPABASE)
# =====================================================
import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import os
import sqlite3
import subprocess
import altair as alt
import plotly.graph_objects as go
import calendar

# =====================================================
# CONFIGURACIÓN GENERAL Y PERSISTENCIA (SUPABASE)
# =====================================================

import psycopg2
import psycopg2.extras

st.sidebar.markdown("### 🗄️ Base de datos en uso")
st.sidebar.code("Supabase · PostgreSQL")

# -----------------------------------------------------
# CONEXIÓN A SUPABASE (PostgreSQL)
# -----------------------------------------------------
def get_conn():
    try:
        conn = psycopg2.connect(
            host=st.secrets["DB_HOST"],
            database=st.secrets["DB_NAME"],
            user=st.secrets["DB_USER"],
            password=st.secrets["DB_PASSWORD"],
            port=int(st.secrets["DB_PORT"]),
            sslmode="require",
        )
        return conn
    except Exception as e:
        st.error("❌ Error conectando a Supabase")
        st.code(str(e))
        st.stop()

# -----------------------------------------------------
# EJECUCIÓN SQL SEGURA
# -----------------------------------------------------
def ejecutar_sql(sql, params=None, fetch=False):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if fetch:
                result = cur.fetchall()
            else:
                result = None
        conn.commit()
        return result
    except Exception as e:
        conn.rollback()
        raise RuntimeError(
            f"""
❌ ERROR SQL (SUPABASE)

SQL:
{sql}

PARAMS:
{params}

ERROR:
{e}
"""
        )
    finally:
        conn.close()

# -----------------------------------------------------
# CONSTANTES DE NEGOCIO
# -----------------------------------------------------
PUNTOS = ["Entrada Planta", "X-507", "Salida FCA"]
PARAMETROS = ["HC", "SS", "DQO", "Sulf"]

LIMITES = {
    "HC": {"puntual": 15, "anual": 2.5},
    "DQO": {"puntual": 700, "anual": 125},
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

ejecutar_sql("""
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

ejecutar_sql("""
CREATE TABLE IF NOT EXISTS envio_emisario (
    dia TEXT PRIMARY KEY,
    envio_emisario INTEGER NOT NULL CHECK (envio_emisario IN (0, 1))
)
""")

ejecutar_sql("""
CREATE TABLE IF NOT EXISTS estimados_upa (
    anio INTEGER NOT NULL,
    parametro TEXT NOT NULL,
    valor REAL NOT NULL,
    PRIMARY KEY (anio, parametro)
)
""")

# =====================================================
# CARGA DE DATOS
# =====================================================

conn = get_conn()

df = pd.read_sql(
    "SELECT * FROM analiticas",
    conn,
    parse_dates=["datetime"]
)

if not df.empty:
    df["dia"] = df["datetime"].dt.date
else:
    df["dia"] = []

df_envio = pd.read_sql(
    "SELECT * FROM envio_emisario",
    conn
)

if not df_envio.empty:
    df_envio["dia"] = pd.to_datetime(df_envio["dia"]).dt.date

# IMPORTANTE: cerrar conexión de lectura
conn.close()

# -----------------------------------------------------
# ESTIMADOS UPA PERSISTENTES
# -----------------------------------------------------
conn_est = get_conn()

df_est = pd.read_sql(
    "SELECT * FROM estimados_upa WHERE anio = ?",
    conn_est,
    params=(anio,)
)

conn_est.close()

def get_estimado(param):
    fila = df_est[df_est["parametro"] == param]
    if not fila.empty:
        return float(fila.iloc[0]["valor"])
    return None

# =====================================================
# FUNCIONES DE NEGOCIO
# =====================================================

def asegurar_datetime(df):
    """
    Garantiza que el DataFrame tiene una columna 'datetime'
    para ordenación y gráficos.
    """
    if "datetime" in df.columns:
        return df

    df = df.copy()

    if "dia" in df.columns:
        df["datetime"] = pd.to_datetime(df["dia"])
        return df

    # Último recurso: no se puede ordenar
    return df

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

def valor_con_semaforo(valor, unidad, limite_anual):
    if valor is None or pd.isna(valor):
        return "—"
    sem = semaforo_promedio(valor, limite_anual)
    return f"{valor:.2f} {unidad} {sem}"

def texto_margen(margen):
    if margen is None:
        return ""
    if margen < 0:
        return f":red[Margen previsto: {margen:.1f} ppm]"
    if margen < 0.2 * abs(margen):
        return f":orange[Margen previsto: +{margen:.1f} ppm]"
    return f":green[Margen previsto: +{margen:.1f} ppm]"

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
        for punto in ["Entrada Planta", "X-507", "Salida FCA"]:
            df_p = g[g["punto"] == punto]
            if not df_p.empty:
                valores[punto] = df_p.sort_values("datetime").iloc[-1][parametro]

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
    df_salida = df_salida.sort_values("datetime")

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
        ent_vals = df_ent.sort_values("datetime")[parametro].tail(5)
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

# =====================================================
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
                
                    if dias_transcurridos == 0:
                        st.info("No hay suficientes datos para calcular la UPA.")
                    else:
                        # -------------------------------------------------
                        # Cargar estimados persistentes (desde BBDD)
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
                        # Guardar estimados UPA (EXPLÍCITO Y SEGURO)
                        # -------------------------------------------------
                        if st.button("💾 Guardar estimados UPA"):
                        
                            # Normalizar valores (SQLite no acepta NaN)
                            est_hc_sql = float(est_hc) if est_hc is not None and not pd.isna(est_hc) else None
                            est_dqo_sql = float(est_dqo) if est_dqo is not None and not pd.isna(est_dqo) else None
                        
                            if est_hc_sql is not None:
                                ejecutar_sql(
                                    """
                                    INSERT OR REPLACE INTO estimados_upa (anio, parametro, valor)
                                    VALUES (?, ?, ?)
                                    """,
                                    (anio, "HC", est_hc_sql)
                                )
                        
                            if est_dqo_sql is not None:
                                ejecutar_sql(
                                    """
                                    INSERT OR REPLACE INTO estimados_upa (anio, parametro, valor)
                                    VALUES (?, ?, ?)
                                    """,
                                    (anio, "DQO", est_dqo_sql)
                                )
                        
                            if est_hc_sql is None and est_dqo_sql is None:
                                st.warning("No hay valores válidos para guardar.")
                            else:
                                st.success("Estimados UPA guardados correctamente")
                                st.rerun()
                                                                
                        # -------------------------------------------------
                        # Cálculo UPA
                        # -------------------------------------------------
                        upa_hc = calcular_upa(
                            hc_anual,
                            dias_transcurridos,
                            est_hc,
                            dias_restantes
                        )
                
                        upa_dqo = calcular_upa(
                            dqo_anual,
                            dias_transcurridos,
                            est_dqo,
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
                    ["HC", "DQO"],
                    key="graf_anual_param"
                )

                limite = LIMITES[parametro]["anual"]

                df_val["mes"] = df_val["dia"].apply(lambda d: d.month)

                prom_mensual = (
                    df_val.groupby("mes")[parametro]
                    .mean()
                    .reindex(range(1, 13))
                )

                prom_acum = prom_mensual.expanding().mean()

                # --------- PROYECCIÓN UPA PROGRESIVA (CORREGIDA) ---------
                proy = prom_acum.copy()
                
                meses_reales = prom_mensual.dropna()
                
                if not meses_reales.empty:
                    ultimo_mes = meses_reales.index.max()
                
                    # Valor estimado mensual
                    est = est_hc if parametro == "HC" else est_dqo
                
                    # Suma y número de meses reales
                    suma_real = meses_reales.sum()
                    n_real = len(meses_reales)
                
                    for m in range(ultimo_mes + 1, 13):
                        suma_real += est
                        n_real += 1
                        proy.loc[m] = suma_real / n_real
                        
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
                    yaxis_title="ppm",
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
        df_val = df_val.sort_values("datetime")
    
        ultima = df_val.iloc[-1]
    
        fecha_txt = ultima["datetime"].strftime("%d/%m/%Y %H:%M")
    
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("HC", ultima["HC"])
        c2.metric("DQO", ultima["DQO"])
        c3.metric("SS", ultima["SS"])
        c4.metric("Sulf", ultima["Sulf"])
    
        st.caption(f"📅 Último análisis disponible: **{fecha_txt}**")
    
        estado = estado_global(ultima["HC"], ultima["DQO"])
    
        if estado.startswith("🔴"):
            st.error(f"Estado global: {estado}")
        elif estado.startswith("🟠"):
            st.warning(f"Estado global: {estado}")
        else:
            st.success(f"Estado global: {estado}")

    # ---------- GRÁFICOS (MEJORADOS) ----------
    st.subheader("📈 Análisis gráfico")

    # -------------------------------------------------
    # Selectores
    # -------------------------------------------------
    c1, c2, c3, c4 = st.columns(4)
    
    punto_sel = c1.selectbox(
        "Punto",
        ["Entrada Planta", "X-507", "Salida FCA", "Comparativo"],
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
            }
    
            for p in PUNTOS:
                df_p = df_plot[df_plot["punto"] == p].copy()
                if df_p.empty:
                    continue
    
                if p == "Salida FCA":
                    df_p = analitica_valida_salida_fca(df_p)
    
                fig.add_trace(
                    go.Scatter(
                        x=df_p["datetime"],
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
        
            # 🔧 Asegurar columna datetime para gráficos
            df_p = asegurar_datetime(df_p)
        
            if "datetime" in df_p.columns:
                df_p = df_p.sort_values("datetime")
            else:
                st.warning("⚠️ No se puede ordenar: falta columna datetime")   

            if not df_p.empty and "datetime" in df_p.columns:
            
                fig.add_trace(
                    go.Scatter(
                        x=df_p["datetime"],
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
                        x=df_p["datetime"],
                        y=df_p["EMA7"],
                        mode="lines",
                        name="EMA 7",
                        line=dict(color="#F58518", width=3)
                    )
                )
    
        # -------------------------------------------------
        # Límites legales
        # -------------------------------------------------
        if param_sel in LIMITES:
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
    
    else:
        st.info("No hay datos para el gráfico")

    # -------------------------------------------------
    # Eficiencias
    # -------------------------------------------------
    
    st.markdown(f"### 🧪 Eficiencia de eliminación diaria – {param_sel}")
    st.caption(f"Porcentaje de eliminación de {param_sel} · cálculo diario")

    df_eff = calcular_eficiencias_diarias(df_plot, param_sel)
    
    if not df_eff.empty:
    
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
    st.markdown(f"### 🧠 Diagnóstico automático – {param_sel}")
    
    def diagnostico_filtros_fca(df_plot, parametro):
        """
        Diagnóstico automático de filtros FCA
        Basado en tendencia reciente (EMA7) y eficiencia real
        """
    
        resultado = {
            "estado": "🟢 Normal",
            "mensaje": "Funcionamiento dentro de parámetros normales.",
            "motivos": []
        }
    
        if parametro not in ["HC", "DQO"]:
            return resultado
    
        # ---------- SALIDA FCA ----------
        df_salida = df_plot[df_plot["punto"] == "Salida FCA"].copy()
        if df_salida.empty or len(df_salida) < 7:
            return resultado
    
        df_salida = analitica_valida_salida_fca(df_salida)
        df_salida = df_salida.sort_values("datetime")
    
        # ---------- EMA 7 ----------
        df_salida["EMA7"] = df_salida[parametro].ewm(span=7, adjust=False).mean()
        ema_diff = df_salida["EMA7"].diff().dropna()
    
        ema_diff_reciente = ema_diff.tail(5)
        subidas_recientes = (ema_diff_reciente > 0).sum()
    
        # ---------- EFICIENCIA ----------
        df_eff = calcular_eficiencias_diarias(df_plot, parametro)
        eff_reciente = df_eff["E_X507_Salida"].dropna().tail(5)
    
        eff_media = eff_reciente.mean() if not eff_reciente.empty else None
        eff_tendencia = eff_reciente.diff().mean() if len(eff_reciente) > 1 else 0
    
        # ---------- ENTRADA ESTABLE ----------
        df_ent = df_plot[df_plot["punto"] == "Entrada Planta"]
        entrada_estable = True
        if not df_ent.empty:
            ent_vals = df_ent.sort_values("datetime")[parametro].tail(5)
            if ent_vals.max() - ent_vals.min() > ent_vals.mean() * 0.2:
                entrada_estable = False
    
        # ---------- LÓGICA FINAL ----------
        if (
            subidas_recientes >= 3 and
            eff_media is not None and eff_media < 60 and
            eff_tendencia < 0 and
            entrada_estable
        ):
            resultado["estado"] = "🔴 Posible limpieza de filtros"
            resultado["mensaje"] = "Deriva negativa sostenida en salida FCA."
            resultado["motivos"] = [
                "Tendencia reciente ascendente en salida FCA (EMA 7)",
                "Eficiencia en descenso en etapa X-507 → Salida FCA",
                "Entrada estable"
            ]
    
        elif subidas_recientes >= 2 and eff_tendencia < 0:
            resultado["estado"] = "🟠 Vigilancia"
            resultado["mensaje"] = "Se detectan señales tempranas de desviación."
            resultado["motivos"] = [
                "Tendencia reciente al alza",
                "Ligera caída de eficiencia"
            ]
    
        return resultado

    # -------------------------------------------------
    # 🧠 MOSTRAR RESULTADO DEL DIAGNÓSTICO
    # -------------------------------------------------
    diag = diagnostico_filtros_fca(df_plot, param_sel)
    
    if diag is None:
        st.info("No hay datos suficientes para el diagnóstico automático.")
    else:
        if diag["estado"].startswith("🔴"):
            st.error(f"{diag['estado']}\n\n{diag['mensaje']}")
        elif diag["estado"].startswith("🟠"):
            st.warning(f"{diag['estado']}\n\n{diag['mensaje']}")
        else:
            st.success(f"{diag['estado']}\n\n{diag['mensaje']}")
    
        if diag.get("motivos"):
            st.markdown("**Motivos detectados:**")
            for m in diag["motivos"]:
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
    
        if st.button("Guardar analítica"):
            dt = datetime.combine(fecha, hora).strftime("%Y-%m-%d %H:%M:%S")
    
            ejecutar_sql(
                """
                INSERT OR REPLACE INTO analiticas
                (datetime, punto, HC, SS, DQO, Sulf)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (dt, punto, hc, ss, dqo, sulf)
            )
    
            st.success("Analítica guardada correctamente")
    
    
    # ---------- TABLA EDITABLE ----------
    with st.expander("📊 Tabla de analíticas"):
        if not df.empty:
            df_edit = st.data_editor(
                df.drop(columns=["dia"]),
                use_container_width=True,
                hide_index=True,
            )
    
            if st.button("Guardar cambios en tabla"):
                # Vaciar tabla
                ejecutar_sql("DELETE FROM analiticas")
    
                # Reinsertar fila a fila (persistencia garantizada)
                for _, row in df_edit.iterrows():
                    ejecutar_sql(
                        """
                        INSERT INTO analiticas
                        (datetime, punto, HC, SS, DQO, Sulf)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            row["datetime"],
                            row["punto"],
                            row["HC"],
                            row["SS"],
                            row["DQO"],
                            row["Sulf"],
                        ),
                    )
    
                st.success("Tabla actualizada correctamente")
    
    
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
                ejecutar_sql("DELETE FROM envio_emisario")
    
                for _, row in tabla_edit.iterrows():
                    ejecutar_sql(
                        """
                        INSERT INTO envio_emisario (dia, envio_emisario)
                        VALUES (?, ?)
                        """,
                        (
                            row["dia"].strftime("%Y-%m-%d"),
                            int(row["envio_emisario"]),
                        ),
                    )
    
                st.success("Envío a emisario actualizado correctamente")      
        
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
    
                    ejecutar_sql(
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
    
            st.success(
                f"Importación completada: {total_insertados} registros procesados"
            )
            st.rerun()
    
    # ---------- COPIA DE SEGURIDAD BBDD (GITHUB) ----------
    with st.expander("💾 Copia de seguridad (GitHub)"):
    
        st.markdown(
            """
            La base de datos **planta.db** se guarda directamente en el repositorio GitHub.
            
            ✔️ Los datos **no se pierden al reiniciar la app**  
            ✔️ GitHub actúa como **backup histórico**  
            ✔️ Se puede volver a versiones anteriores si es necesario
            """
        )
    
        st.divider()
    
        with st.expander("💾 Persistencia de datos"):
            st.info(
                """
                ℹ️ Los datos se guardan en el archivo **planta.db** dentro del repositorio.
        
                Para que los datos persistan tras reiniciar la aplicación,
                es necesario confirmar los cambios en GitHub manualmente:
        
                ```bash
                git pull --rebase
                git add data/planta.db
                git commit -m "Actualizar base de datos analíticas"
                git push

                ```
                """
            )

            st.info(
                    """
                    ℹ️ Los datos se guardan en **data/planta.db**.
                    Antes de hacer commit en GitHub, pulsa el botón:
                    """
                )
            
            if st.button("🔒 Preparar base de datos para commit Git"):
                st.success(
                    "ℹ️ La base de datos ya está sincronizada.\n\n"
                    "Si vas a hacer commit, detén la app o reiníciala antes."
                )
                               
            st.divider()
        
            st.info(
                "ℹ️ Recomendación: guarda en GitHub al final de cada jornada "
                "o tras introducir/modificar analíticas importantes."
            )

        # Descargar último backup
        with st.expander("💾 Copia de seguridad"):
        
            st.info(
                "La base de datos se guarda en **data/planta.db** "
                "y se versiona mediante GitHub."
            )
        
            if os.path.exists(DB_PATH):
                with open(DB_PATH, "rb") as f:
                    st.download_button(
                        "⬇️ Descargar base de datos actual",
                        data=f,
                        file_name="planta.db",
                        mime="application/octet-stream"
                    )
                
        # --- IMPORTAR / RESTAURAR ---
        uploaded_db = st.file_uploader(
            "📤 Restaurar base de datos desde backup (.db)",
            type=["db"],
            key="upload_db_backup"
        )

        if uploaded_db is not None:
            st.warning(
                "⚠️ Esta acción sobrescribirá TODOS los datos actuales."
            )

            if st.button("🔁 Restaurar base de datos", key="restore_db_btn"):
                with open(DB_PATH, "wb") as f:
                    f.write(uploaded_db.read())

                st.success("Base de datos restaurada correctamente.")
                st.info("La aplicación se recargará para aplicar los cambios.")
                st.rerun()

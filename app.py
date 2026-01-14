# =====================================================
# app.py – v2.6 COMPLETO, ESTABLE Y UX LIMPIO
# =====================================================

import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import os
import sqlite3
import altair as alt
import plotly.graph_objects as go

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

    # =====================================================
    # 📐 HC y DQO acumulados – Salida FCA
    # =====================================================
    st.subheader("📐 HC y DQO acumulados – Salida FCA")
    
    if df.empty or df_envio.empty:
        st.info("No hay datos suficientes para calcular los promedios.")
    else:
        # --- Última analítica válida diaria de Salida FCA ---
        df_val = analitica_valida_salida_fca(
            df[df["punto"] == "Salida FCA"]
        )
    
        # --- Solo días con envío a emisario ---
        dias_envio = df_envio[df_envio["envio_emisario"] == 1]["dia"]
        df_val = df_val[df_val["dia"].isin(dias_envio)]
    
        if df_val.empty:
            st.warning("No hay días con envío a emisario y analítica válida.")
        else:
            hoy = date.today()
            mes_actual = hoy.month
            anio_actual = hoy.year
    
            # --- Promedio acumulado mensual ---
            df_mes = df_val[
                (df_val["dia"].apply(lambda d: d.month) == mes_actual) &
                (df_val["dia"].apply(lambda d: d.year) == anio_actual)
            ]
    
            # --- Promedio acumulado anual ---
            df_anual = df_val[
                df_val["dia"].apply(lambda d: d.year) == anio_actual
            ]
    
            # --- Valores ---
            hc_mes = df_mes["HC"].mean() if not df_mes.empty else None
            dqo_mes = df_mes["DQO"].mean() if not df_mes.empty else None
            hc_anual = df_anual["HC"].mean() if not df_anual.empty else None
            dqo_anual = df_anual["DQO"].mean() if not df_anual.empty else None
    
            c1, c2, c3, c4 = st.columns(4)
    
            # --- Mensual ---
            c1.metric(
                "HC medio mensual (ppm)",
                f"{hc_mes:.2f}" if hc_mes is not None else "—",
                semaforo_promedio(hc_mes, LIMITES["HC"]["anual"])
            )
            c2.metric(
                "DQO medio mensual (ppm)",
                f"{dqo_mes:.2f}" if dqo_mes is not None else "—",
                semaforo_promedio(dqo_mes, LIMITES["DQO"]["anual"])
            )
    
            # --- Anual ---
            c3.metric(
                "HC medio anual (ppm)",
                f"{hc_anual:.2f}" if hc_anual is not None else "—",
                semaforo_promedio(hc_anual, LIMITES["HC"]["anual"])
            )
            c4.metric(
                "DQO medio anual (ppm)",
                f"{dqo_anual:.2f}" if dqo_anual is not None else "—",
                semaforo_promedio(dqo_anual, LIMITES["DQO"]["anual"])
            )

    # =====================================================
# 🔮 UPA – Última previsión anual
# =====================================================
st.divider()
st.subheader("🔮 UPA – Última previsión anual (Salida FCA)")

# --- Días con envío a emisario ya considerados ---
df_anual_env = df_val.copy()

dias_transcurridos = len(df_anual_env)
dias_totales = 365
dias_restantes = max(dias_totales - dias_transcurridos, 0)

if dias_transcurridos == 0:
    st.info("No hay suficientes datos para calcular la UPA.")
else:
    c1, c2 = st.columns(2)

    with c1:
        est_hc = st.number_input(
            "Estimado HC medio hasta fin de año (ppm)",
            min_value=0.0,
            value=float(hc_anual) if hc_anual is not None else 0.0,
            step=0.1
        )

    with c2:
        est_dqo = st.number_input(
            "Estimado DQO medio hasta fin de año (ppm)",
            min_value=0.0,
            value=float(dqo_anual) if dqo_anual is not None else 0.0,
            step=1.0
        )

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

    c3, c4 = st.columns(2)

    c3.metric(
        "UPA HC (ppm)",
        f"{upa_hc:.2f}" if upa_hc is not None else "—",
        semaforo_promedio(upa_hc, LIMITES["HC"]["anual"])
    )

    c4.metric(
        "UPA DQO (ppm)",
        f"{upa_dqo:.2f}" if upa_dqo is not None else "—",
        semaforo_promedio(upa_dqo, LIMITES["DQO"]["anual"])
    )

    
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
    
    # --- Límites legales como líneas horizontales (ESTABLE) ---
    if param_sel in LIMITES and not df_plot.empty:
        limites_df = pd.DataFrame({
            "limite": ["Anual", "Puntual"],
            "valor": [
                LIMITES[param_sel]["anual"],
                LIMITES[param_sel]["puntual"],
            ],
        })
    
        capas.append(
            alt.Chart(limites_df).mark_rule(
                strokeWidth=2,
                strokeDash=[6, 4]
            ).encode(
                y="valor:Q",
                color=alt.Color(
                    "limite:N",
                    scale=alt.Scale(
                        domain=["Anual", "Puntual"],
                        range=["orange", "red"]
                    ),
                    legend=alt.Legend(title="Límites legales")
                )
            )
        )

    if not df_plot.empty:
    
        fig = go.Figure()
    
        # --- Líneas de datos ---
        if punto_sel == "Comparativo":
            colores = {
                "Entrada Planta": "blue",
                "X-507": "orange",
                "Salida FCA": "green",
            }
            for p in PUNTOS:
                df_p = df_plot[df_plot["punto"] == p]
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
        else:
            df_p = df_plot[df_plot["punto"] == punto_sel]
            if punto_sel == "Salida FCA":
                df_p = analitica_valida_salida_fca(df_p)
    
            fig.add_trace(
                go.Scatter(
                    x=df_p["datetime"],
                    y=df_p[param_sel],
                    mode="lines+markers",
                    name=punto_sel
                )
            )
    
        # --- Límites legales ---
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
    
        fig.update_layout(
            height=450,
            margin=dict(l=40, r=40, t=40, b=40),
            xaxis_title="Fecha",
            yaxis_title=param_sel,
            legend_title="Punto",
        )
    
        st.plotly_chart(fig, use_container_width=True)

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

    # ---------- COPIA DE SEGURIDAD BBDD ----------
    with st.expander("💾 Copia de seguridad de la base de datos"):
        st.markdown(
            """
            **Backup / restauración de la base de datos**

            Recomendado:
            - antes de cerrar un mes
            - antes de reimportar Excel
            - antes de grandes ediciones
            """
        )

        # --- EXPORTAR ---
        try:
            with open(DB_PATH, "rb") as f:
                st.download_button(
                    label="📥 Descargar backup de la BBDD",
                    data=f,
                    file_name="planta_backup.db",
                    mime="application/octet-stream",
                    key="download_db_backup"
                )
        except Exception as e:
            st.warning("No se ha podido acceder a la base de datos para el backup")

        st.divider()

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

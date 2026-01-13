import streamlit as st
st.title("💧 Control de analíticas – Planta de tratamiento de aguas")


# ---------------- DATA ----------------
# ---------------- DATA ----------------
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

# ---------------- INPUT ----------------
st.subheader("📝 Introducción manual de datos")
with st.form("form"):
c1, c2, c3 = st.columns(3)
fecha = c1.date_input("Fecha")
hora = c1.time_input("Hora")
punto = c2.selectbox("Punto", PUNTOS)
envio = c2.checkbox("Envío a emisario")
hc = c3.number_input("HC", 0.0)
ss = c3.number_input("SS", 0.0)
dqo = c3.number_input("DQO", 0.0)
ph = c3.number_input("PH", 0.0)


if st.form_submit_button("Guardar"):
row = {
"datetime": datetime.combine(fecha, hora),
"punto": punto,
"HC": hc,
"SS": ss,
"DQO": dqo,
"PH": ph,
"envio_emisario": envio
}
df = pd.concat([df, pd.DataFrame([row])])
df.to_csv(DATA_FILE, index=False)
st.success("Dato guardado")


# ---------------- TABLE ----------------
st.subheader("📊 Registros")
st.dataframe(df.sort_values("datetime", ascending=False), use_container_width=True)


# ---------------- GRAPHS ----------------
st.subheader("📈 Gráficos")
col1, col2 = st.columns(2)
punto_sel = col1.selectbox("Punto", PUNTOS)
param_sel = col2.selectbox("Parámetro", PARAMETROS)


df_g = df[df["punto"] == punto_sel]


base = alt.Chart(df_g).encode(x="datetime:T")
line = base.mark_line(point=True).encode(y=f"{param_sel}:Q")


layers = [line]


if param_sel in LIMITES:
lim = LIMITES[param_sel]
layers.append(alt.Chart(pd.DataFrame({"y": [lim["puntual"]]})).mark_rule(color="red").encode(y="y:Q"))
layers.append(alt.Chart(pd.DataFrame({"y": [lim["anual"]]})).mark_rule(color="orange", strokeDash=[6,4]).encode(y="y:Q"))


chart = alt.layer(*layers).properties(height=420)
st.altair_chart(chart, use_container_width=True)


# ---------------- PROMEDIOS ----------------
st.subheader("📐 Promedios acumulados (Salida FCA + Envío a emisario)")
df_p = df[(df.punto == "Salida FCA") & (df.envio_emisario == True)]


if not df_p.empty:
st.metric("HC promedio", f"{df_p.HC.mean():.2f} ppm")
st.metric("DQO promedio", f"{df_p.DQO.mean():.2f} ppm")


# ---------------- PDF ----------------
st.subheader("📄 Informe mensual PDF")


if not df.empty:
meses = sorted(df.datetime.dt.to_period("M").astype(str).unique())
mes_sel = st.selectbox("Mes", meses)


if st.button("Generar PDF"):
file = f"{REPORT_DIR}/informe_{mes_sel}.pdf"
c = canvas.Canvas(file, pagesize=A4)
text = c.beginText(40, 800)
text.textLine(f"Informe mensual – {mes_sel}")
text.textLine("")
df_m = df[df.datetime.dt.to_period("M").astype(str) == mes_sel]
for _, r in df_m.iterrows():
text.textLine(f"{r.datetime} | {r.punto} | HC {r.HC} | DQO {r.DQO}")
c.drawText(text)
c.save()
st.success(f"Informe generado: {file}")

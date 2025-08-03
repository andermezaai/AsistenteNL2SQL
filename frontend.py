import streamlit as st
import pandas as pd
from backend import (
    conectar_sql_server,
    obtener_esquema,
    obtener_plantilla,
    procesar_consulta_nl2sql
)

st.set_page_config(page_title="Asistente NL2SQL", layout="wide")

st.title("🤖 Asistente de Consultas SQL desde Lenguaje Natural")

# -----------------------
# Sección 1: Autenticación
# -----------------------

st.sidebar.header("🔐 Conexión a la Base de Datos")

with st.sidebar.form("form_conexion"):
    servidor = st.text_input("Servidor", value="localhost\\SQLEXPRESS")
    usuario = st.text_input("Usuario", value="sa")
    contrasena = st.text_input("Contraseña", type="password")
    base_datos = st.text_input("Base de Datos", value="TPC_H")
    conectar_btn = st.form_submit_button("Conectar")

# Guardar conexión y esquema en sesión
if "conexion" not in st.session_state:
    st.session_state.conexion = None
if "esquema" not in st.session_state:
    st.session_state.esquema = None

if conectar_btn:
    try:
        conexion = conectar_sql_server(servidor, usuario, contrasena, base_datos)
        st.session_state.conexion = conexion
        st.success("Conexión exitosa 🎉")

        # Extraer esquema solo una vez
        with st.spinner("Extrayendo esquema de la base de datos..."):
            esquema = obtener_esquema(conexion)
            st.session_state.esquema = esquema
            st.success("Esquema cargado correctamente ✅")
    except Exception as e:
        st.error(f"❌ Error al conectar: {str(e)}")

# -----------------------
# Sección 2: Consulta NL2SQL
# -----------------------

if st.session_state.conexion and st.session_state.esquema:
    st.subheader("💬 Realiza tu pregunta sobre la base de datos")
    pregunta = st.text_input("Pregunta en lenguaje natural")

    if "plantilla_sql" not in st.session_state:
        st.session_state.plantilla_sql = obtener_plantilla()

    if st.button("Generar y ejecutar consulta"):
        if pregunta.strip() == "":
            st.warning("Por favor, ingresa una pregunta.")
        else:
            with st.spinner("Procesando..."):
                resultado = procesar_consulta_nl2sql(
                    pregunta=pregunta,
                    conexion=st.session_state.conexion,
                    esquema=st.session_state.esquema,
                    plantilla_sql=st.session_state.plantilla_sql
                )

            if resultado["error"]:
                st.error(resultado["error"])
                if resultado["consulta_sql"]:
                    with st.expander("🔍 Consulta Generada"):
                        st.code(resultado["consulta_sql"], language="sql")
            else:
                st.success("Consulta ejecutada con éxito ✅")
                with st.expander("🔍 Consulta Generada"):
                    st.code(resultado["consulta_sql"], language="sql")
                st.dataframe(resultado["dataframe"], use_container_width=True)

                # Descargar CSV
                csv = resultado["dataframe"].to_csv(index=False).encode("utf-8")
                st.download_button("📥 Descargar resultados", data=csv, file_name="resultado.csv", mime="text/csv")

import os
import re
import pyodbc
import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI
from langchain.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from langchain.chains import LLMChain

load_dotenv()

rutaPlantilla="plantillaSQL.txt"
model="gpt-4o"


def obtener_plantilla(ruta: str = rutaPlantilla):
    with open(rutaPlantilla, "r", encoding="utf-8") as file:
        contenido_prompt = file.read()
    return PromptTemplate.from_template(contenido_prompt)

def conectar_sql_server(server: str, user: str, password: str, database: str):
    """
    Establece conexión a SQL Server y retorna el objeto de conexión.
    """
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={database};UID={user};PWD={password};"
        "TrustServerCertificate=yes"
    )
    conexion = pyodbc.connect(conn_str)
    print("✅ Conexión exitosa a la base de datos.")
    return conexion
    
def obtener_esquema(conexion):
    """
    Extrae nombres de tablas, columnas y tipos de datos.
    """
    textoContexto=""

    queryColumnas ="""
    SELECT 
        TABLE_NAME,
        COLUMN_NAME,
        DATA_TYPE
    FROM 
        INFORMATION_SCHEMA.COLUMNS
    WHERE
        TABLE_NAME <> 'sysdiagrams'
    """
    queryRelaciones ="""
    SELECT 
        tp.name AS Parent_Table,
        cp.name AS Parent_Column,
        tr.name AS Referenced_Table,
        cr.name AS Referenced_Column
    FROM 
        sys.foreign_keys fk
    INNER JOIN 
        sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
    INNER JOIN 
        sys.tables tp ON tp.object_id = fk.parent_object_id
    INNER JOIN 
        sys.columns cp ON fkc.parent_column_id = cp.column_id AND cp.object_id = tp.object_id
    INNER JOIN 
        sys.tables tr ON tr.object_id = fk.referenced_object_id
    INNER JOIN 
        sys.columns cr ON fkc.referenced_column_id = cr.column_id AND cr.object_id = tr.object_id
    ORDER BY 
        Parent_Table, Parent_Column, Referenced_Table, Referenced_Column
    """
    dfColumnas = pd.read_sql(queryColumnas, conexion)
    dfRelaciones = pd.read_sql(queryRelaciones, conexion)

    textoTablas = ""
    for tabla, grupo in dfColumnas.groupby('TABLE_NAME'):
        columnas = ", ".join(f"{row['COLUMN_NAME']}({row['DATA_TYPE']})" for _, row in grupo.iterrows())
        textoTablas += f"- {tabla}: {columnas}\n"

    textoRelaciones = ""
    for _, row in dfRelaciones.iterrows():
        textoRelaciones += f"- {row['Parent_Table']}({row['Parent_Column']}) -> {row['Referenced_Table']}({row['Referenced_Column']})\n"

    textoContexto+=f"Tablas y columnas:\n{textoTablas}\nRelaciones entre tablas:(clave foránea -> clave primaria)\n{textoRelaciones}"

    return textoContexto

def validar_pregunta_relevante(pregunta: str, esquema: str,model: str=model):
    """
    Valida si la pregunta del usuario es coherente, relacionada con el esquema
    y puede responderse mediante SQL. Devuelve (es_valida, mensaje).
    """
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    prompt = f"""
        Eres un asistente que ayuda a interpretar preguntas de usuarios sobre una base de datos.
        Dado el siguiente esquema de base de datos y una pregunta del usuario, indica si la pregunta
        tiene sentido, está relacionada con el dominio del esquema, y puede ser respondida mediante SQL.

        Esquema:
        {esquema}

        Pregunta:
        "{pregunta}"

        Responde solo con "SI" si la pregunta es válida o "NO" si no lo es. No añadas más información.
    """

    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    respuesta = response.choices[0].message.content.strip()

    if respuesta.lower().startswith("si"):
        return True
    else:
        return False
    
def generar_consulta_sql(pregunta: str, esquema: str, plantilla_sql:str, model:str=model):
    """
    Genera una consulta SQL usando LangChain a partir de una pregunta en lenguaje natural.
    """
    llm = ChatOpenAI(model_name=model, temperature=0)

    chain_sql = LLMChain(
        llm=llm,
        prompt=plantilla_sql
    )

    respuesta = chain_sql.run({
        "pregunta": pregunta,
        "esquema": esquema
    })

    return respuesta.strip()


def es_consulta_segura(sql: str):
    """
    Verifica que la consulta sea solo de lectura.
    """
    patron_prohibido = re.compile(r"\b(DELETE|UPDATE|INSERT|DROP|ALTER|TRUNCATE|CREATE|MERGE)\b", re.IGNORECASE)
    return not patron_prohibido.search(sql)

def ejecutar_consulta(conexion, consulta_sql):
    """
    Valida y ejecuta la consulta SQL. Devuelve un DataFrame con resultados o mensaje de error.
    """
    try:
        df_resultado = pd.read_sql(consulta_sql, conexion)
        return df_resultado, None
    except Exception as e:
        return None, f"❌ Error al ejecutar la consulta: {str(e)}"
    
def procesar_consulta_nl2sql(pregunta: str, conexion, esquema: str, plantilla_sql: PromptTemplate, model: str=model):
    """
    Orquesta todo el pipeline NL2SQL:
    1. Valida la pregunta
    2. Genera consulta SQL
    3. Verifica seguridad
    4. Ejecuta la consulta

    Retorna un diccionario con claves:
    - 'consulta_sql'
    - 'dataframe'
    - 'error'
    """
    # Paso 1: Validar pregunta
    if not validar_pregunta_relevante(pregunta, esquema, model):
        return {
            "consulta_sql": None,
            "dataframe": None,
            "error": "❌ La pregunta no es válida o no está relacionada con la base de datos."
        }
    print("✅ Pregunta válida, procediendo a generar consulta SQL...")
    # Paso 2: Generar SQL
    consulta_sql = generar_consulta_sql(pregunta, esquema, plantilla_sql, model)

    # Paso 3: Validar seguridad
    if not es_consulta_segura(consulta_sql):
        return {
            "consulta_sql": consulta_sql,
            "dataframe": None,
            "error": "⚠️ La consulta generada no es segura (puede modificar datos)."
        }
    print("✅ Consulta SQL generada:", consulta_sql)
    # Paso 4: Ejecutar SQL
    resultado, error = ejecutar_consulta(conexion, consulta_sql)
    return {
        "consulta_sql": consulta_sql,
        "dataframe": resultado,
        "error": error
    }
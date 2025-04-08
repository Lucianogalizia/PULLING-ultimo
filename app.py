# =============================================================================
# Importación de Librerías y Configuración Inicial
# =============================================================================
from flask import Flask, request, redirect, url_for, render_template, flash
import pandas as pd
import numpy as np
import datetime
import os
import re
import unicodedata
from openpyxl import load_workbook
from werkzeug.utils import secure_filename
from geopy.distance import geodesic

# Configuración de la aplicación Flask
app = Flask(__name__)
app.secret_key = "super_secret_key"  # Clave secreta para sesiones y flash

# Carpeta donde se almacenarán los archivos subidos
UPLOAD_FOLDER = "uploads"
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Diccionario global para simular el "estado de sesión"
data_store = {}

# =============================================================================
# Funciones Auxiliares
# =============================================================================
def normalize_text(text):
    """
    Normaliza el texto:
      - Convierte a minúsculas.
      - Elimina acentos.
      - Elimina espacios innecesarios.
    """
    if not isinstance(text, str):
        return text
    text = text.strip().lower()
    # Elimina acentos
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
    # Reemplaza múltiples espacios por uno solo
    text = re.sub(r'\s+', ' ', text)
    return text

def process_excel(file_path):
    """
    Procesa el archivo Excel subido (hoja "dataset") y realiza lo siguiente:
      1. Lee la hoja "dataset" y normaliza los encabezados.
      2. Recorre cada fila en una única iteración para:
         - Descartar filas si la celda "OBSERVACIONES" tiene un color no deseado.
         - Detectar y almacenar en una lista los pozos "celestes" basados en el color de fondo de la columna POZO.
         - Convertir cada fila en un diccionario.
      3. Crea un DataFrame y aplica:
         - Normalización y renombrado de columnas.
         - Ordenamiento (por "Pérdida [m3/d]") y filtrado (por "Plan [Si/No]", campos críticos, palabras no deseadas en "EQUIPO").
      4. Hace merge con el Excel de coordenadas para agregar GEO_LATITUDE y GEO_LONGITUDE.
      5. Verifica que existan coordenadas y renombra/selecciona las columnas finales; añade PROD_DT y RUBRO.
    """
    # --- Leer el Excel principal (hoja "dataset") ---
    wb = load_workbook(file_path, data_only=True)
    if "dataset" not in wb.sheetnames:
        raise ValueError("La hoja 'dataset' no se encontró en el archivo.")
    ws = wb["dataset"]

    # Extraer encabezados y generar mapeo de índices a nombres normalizados
    header = []
    col_map = {}
    for idx, cell in enumerate(ws[1]):
        val = cell.value if cell.value is not None else ""
        norm = normalize_text(val)
        header.append(val)
        col_map[idx] = norm

    # Detectar los índices relevantes: "OBSERVACIONES" y "POZO"
    observaciones_idx = next((idx for idx, col in col_map.items() if "observac" in col), None)
    pozo_idx = next((idx for idx, col in col_map.items() if "pozo" in col), None)

    data = []
    pozos_celestes = []
    # Definir los colores que provocan descartar la fila para la columna "OBSERVACIONES"
    # Aquí se consideran dos códigos: uno (por ejemplo "FF00FFFF") o "FFFF0000"
    colores_descartar = {"FF00FFFF", "FFFF0000"}

    # Recorrer las filas (desde la fila 2) una sola vez
    for row in ws.iter_rows(min_row=2, values_only=False):
        # Filtrar filas según color de fuente en "OBSERVACIONES"
        if observaciones_idx is not None:
            cell_obs = row[observaciones_idx]
            if cell_obs.font and cell_obs.font.color and cell_obs.font.color.rgb:
                if str(cell_obs.font.color.rgb).upper() in colores_descartar:
                    continue  # Se descarta la fila

        # Detectar pozos celestes basados en el color de fondo de la celda en la columna POZO
        if pozo_idx is not None:
            cell_pozo = row[pozo_idx]
            if cell_pozo.fill and cell_pozo.fill.fgColor:
                # Verificar si el tipo de color es 'rgb' y extraer su valor
                if cell_pozo.fill.fgColor.type == "rgb" and cell_pozo.fill.fgColor.rgb:
                    color_code = cell_pozo.fill.fgColor.rgb.upper()
                    # Si es el color que define "celeste", se añade a la lista
                    if color_code == "FF00FFFF" and cell_pozo.value:
                        pozos_celestes.append(cell_pozo.value)

        # Convertir la fila completa en un diccionario (usando el encabezado original)
        row_data = {header[idx]: cell.value for idx, cell in enumerate(row)}
        data.append(row_data)

    # Crear DataFrame a partir de los datos recopilados
    df_main = pd.DataFrame(data)

    # Normalizar los nombres de columnas y renombrarlos según lo esperado
    normalized_columns = {col: normalize_text(col) for col in df_main.columns}
    expected = {
        "activo": "Activo",
        "pozo": "POZO",
        "x": "X",
        "y": "Y",
        "perdida [m3/d]": "Pérdida [m3/d]",
        "plan [si/no]": "Plan [Si/No]",
        "plan [hs/int]": "Plan [Hs/INT]",
        "sea": "SEA",
        "accion": "Acción",
        "ot": "OT",
        "icp": "ICP",
        "requerimientos": "REQUERIMIENTOS",
        "bateria": "Batería",
        "oi": "OI",
        "equipo": "EQUIPO",
        "observaciones": "OBSERVACIONES"
    }
    rename_dict = {}
    for col in df_main.columns:
        norm = normalize_text(col)
        if norm in expected:
            rename_dict[col] = expected[norm]
    df_main.rename(columns=rename_dict, inplace=True)

    # --- Operaciones de filtrado y preview ---
    # Ordenar por "Pérdida [m3/d]" (convertido a numérico) y obtener un preview (20 filas)
    if "Pérdida [m3/d]" in df_main.columns:
        df_main["Pérdida [m3/d]"] = pd.to_numeric(df_main["Pérdida [m3/d]"], errors="coerce")
        df_main.sort_values(by="Pérdida [m3/d]", ascending=False, inplace=True)
    preview_df = df_main.head(20)

    # Filtrar filas donde "Plan [Si/No]" sea igual a 1
    if "Plan [Si/No]" in df_main.columns:
        df_main = df_main[df_main["Plan [Si/No]"] == 1]

    # Eliminar filas con valores nulos, vacíos o 0 en columnas críticas
    cols_criticas = ["Activo", "POZO", "X", "Y", "Pérdida [m3/d]", "Plan [Si/No]", "Plan [Hs/INT]", "EQUIPO"]
    for col in cols_criticas:
        if col in df_main.columns:
            df_main = df_main[df_main[col].notnull()]
            df_main = df_main[df_main[col] != 0]

    # Filtrar filas cuyo valor en "EQUIPO" contenga palabras no deseadas
    if "EQUIPO" in df_main.columns:
        patrones = ["fb", "pesado", "z inyector", "z recupero"]
        df_main = df_main[df_main["EQUIPO"].apply(lambda valor: 
                                                    True if not isinstance(valor, str) 
                                                    else not any(pat in normalize_text(valor) for pat in patrones))]

    # --- Merge con el Excel de coordenadas ---
    # Se asume que 'coordenadas.xlsx' está en el mismo directorio del proyecto
    df_coords = pd.read_excel("coordenadas.xlsx")
    df_coords = df_coords[["POZO", "GEO_LATITUDE", "GEO_LONGITUDE"]]
    # Reemplazar comas por puntos y convertir a float
    df_coords["GEO_LATITUDE"] = df_coords["GEO_LATITUDE"].astype(str).str.replace(",", ".").astype(float)
    df_coords["GEO_LONGITUDE"] = df_coords["GEO_LONGITUDE"].astype(str).str.replace(",", ".").astype(float)

    # Realizar el merge (left join) por la columna "POZO"
    df_merged = df_main.merge(df_coords, on="POZO", how="left")

    # Verificar si faltan coordenadas y avisar al usuario
    missing_pozos = df_merged[(df_merged["GEO_LATITUDE"].isnull()) | (df_merged["GEO_LONGITUDE"].isnull())]["POZO"].unique()
    if len(missing_pozos) > 0:
        flash(f"Atención: No se encontraron coordenadas para los siguientes pozos: {', '.join(missing_pozos)}")
        df_merged = df_merged.dropna(subset=["GEO_LATITUDE", "GEO_LONGITUDE"])

    # --- Selección y renombrado de columnas finales ---
    columnas_requeridas = ["Activo", "POZO", "Pérdida [m3/d]", "Plan [Hs/INT]", "Batería"]
    df_merged = df_merged[[col for col in columnas_requeridas if col in df_merged.columns] + ["GEO_LATITUDE", "GEO_LONGITUDE"]]
    df_merged.rename(columns={
        "Activo": "ZONA",
        "Pérdida [m3/d]": "NETA [M3/D]",
        "Plan [Hs/INT]": "TIEMPO PLANIFICADO",
        "Batería": "BATERÍA"
    }, inplace=True)

    df_merged["PROD_DT"] = datetime.date.today().strftime("%Y-%m-%d")
    df_merged["RUBRO"] = "ESPERA DE TRACTOR"

    orden_final = ["POZO", "NETA [M3/D]", "PROD_DT", "RUBRO", "GEO_LATITUDE", "GEO_LONGITUDE", "BATERÍA", "ZONA", "TIEMPO PLANIFICADO"]
    df_merged = df_merged[[col for col in orden_final if col in df_merged.columns]]

    return df_merged, preview_df, pozos_celestes

# =============================================================================
# Rutas de la Aplicación Flask
# =============================================================================
@app.route("/")
def index():
    return redirect(url_for("upload_file"))

@app.route("/upload", methods=["GET", "POST"])
def upload_file():
    """
    Ruta para subir y procesar el archivo Excel.
      1. Valida que se haya enviado un archivo.
      2. Guarda el archivo en la carpeta UPLOAD_FOLDER.
      3. Llama a process_excel(), que retorna:
         - df_clean: DataFrame final procesado.
         - preview_df: Preview de las primeras 20 filas.
         - pozos_celestes: Lista de pozos detectados como celestes.
      4. Almacena los resultados en data_store.
      5. Muestra el preview al usuario.
    """
    if request.method == "POST":
        if "excel_file" not in request.files:
            flash("No se encontró el archivo en la solicitud.")
            return redirect(request.url)

        file = request.files["excel_file"]
        if file.filename == "":
            flash("No se seleccionó ningún archivo.")
            return redirect(request.url)

        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)

        try:
            df_clean, preview_df, pozos_celestes = process_excel(filepath)
        except Exception as e:
            flash(f"Error al procesar el Excel: {e}")
            return redirect(request.url)

        data_store["df"] = df_clean
        data_store["celeste_pozos"] = pozos_celestes

        flash("Archivo procesado exitosamente. A continuación se muestra un preview (20 filas).")
        preview_html = preview_df.to_html(classes="table table-striped", index=False)
        return render_template("upload_success.html", preview=preview_html)

    return render_template("upload.html")

@app.route("/filter", methods=["GET", "POST"])
def filter_zonas():
    if "df" not in data_store:
        flash("Debes subir un archivo Excel primero.")
        return redirect(url_for("upload_file"))

    df = data_store["df"]
    zonas_disponibles = sorted(df["ZONA"].unique().tolist())

    if request.method == "POST":
        zonas_seleccionadas = request.form.getlist("zonas")
        if not zonas_seleccionadas:
            flash("Debes seleccionar al menos una zona.")
            return redirect(request.url)

        pulling_count = request.form.get("pulling_count", "3")
        try:
            pulling_count = int(pulling_count)
        except ValueError:
            pulling_count = 3

        df_filtrado = df[df["ZONA"].isin(zonas_seleccionadas)].copy()
        data_store["df_filtrado"] = df_filtrado

        pozos = sorted(df_filtrado["POZO"].unique().tolist())
        data_store["pozos_disponibles"] = pozos
        data_store["pulling_count"] = pulling_count

        flash(f"Zonas seleccionadas: {', '.join(zonas_seleccionadas)} | Pullings: {pulling_count}")
        return redirect(url_for("select_pulling"))

    checkbox_html = ""
    for zona in zonas_disponibles:
        checkbox_html += f'<input type="checkbox" name="zonas" value="{zona}"> {zona}<br>'

    return render_template("filter_zonas.html", checkbox_html=checkbox_html)

@app.route("/select_pulling", methods=["GET", "POST"])
def select_pulling():
    if "df_filtrado" not in data_store:
        flash("Debes filtrar las zonas primero.")
        return redirect(url_for("filter_zonas"))

    df_filtrado = data_store["df_filtrado"]
    pozos_disponibles = sorted(df_filtrado["POZO"].unique().tolist())
    pulling_count = data_store.get("pulling_count", 3)
    pozos_celestes = data_store.get("celeste_pozos", [])

    if request.method == "POST":
        pulling_data = {}
        seleccionados = []
        for i in range(1, pulling_count + 1):
            pozo = request.form.get(f"pulling_pozo_{i}")
            pulling_data[f"Pulling {i}"] = {"pozo": pozo, "tiempo_restante": 0.0}
            seleccionados.append(pozo)

        if len(seleccionados) != len(set(seleccionados)):
            flash("Error: No puedes seleccionar el mismo pozo para más de un pulling.")
            return redirect(request.url)

        data_store["pulling_data"] = pulling_data

        todos_pozos = sorted(df_filtrado["POZO"].unique().tolist())
        data_store["pozos_disponibles"] = [p for p in todos_pozos if p not in seleccionados]

        flash("Selección de Pulling confirmada.")
        return redirect(url_for("assign"))

    form_html = ""
    for i in range(1, pulling_count + 1):
        default_pozo = pozos_celestes[i - 1] if (i - 1) < len(pozos_celestes) else None
        select_options = ""
        for pozo in pozos_disponibles:
            if pozo == default_pozo:
                select_options += f'<option value="{pozo}" selected>{pozo}</option>'
            else:
                select_options += f'<option value="{pozo}">{pozo}</option>'

        form_html += f"""
        <h4>Pulling {i}</h4>
        <label>Pozo para Pulling {i}:</label>
        <select name="pulling_pozo_{i}" class="form-select w-50">
          {select_options}
        </select>
        <hr>
        """

    return render_template("select_pulling.html", form_html=form_html)

@app.route("/assign", methods=["GET"])
def assign():
    if "pulling_data" not in data_store:
        flash("Debes seleccionar los pozos para pulling primero.")
        return redirect(url_for("select_pulling"))

    df = data_store["df"]
    pulling_data = data_store["pulling_data"]

    pulling_lista = list(pulling_data.items())
    pozos_ocupados = set()

    # ----------------------
    # Función auxiliar para calcular coeficiente
    # ----------------------
    def calcular_coeficiente(pozo_referencia, pozo_candidato):
        registro_ref = df[df["POZO"] == pozo_referencia].iloc[0]
        registro_cand = df[df["POZO"] == pozo_candidato].iloc[0]
        distancia = geodesic(
            (registro_ref["GEO_LATITUDE"], registro_ref["GEO_LONGITUDE"]),
            (registro_cand["GEO_LATITUDE"], registro_cand["GEO_LONGITUDE"])
        ).kilometers
        neta = registro_cand["NETA [M3/D]"]
        tiempo_plan = registro_cand["TIEMPO PLANIFICADO"]
        denominador = tiempo_plan + 0.5 * distancia
        coeficiente = neta / denominador if denominador != 0 else 0
        return coeficiente, distancia

    def asignar_pozos(pulling_asignaciones, nivel):
        no_asignados = [p for p in data_store["pozos_disponibles"] if p not in pozos_ocupados]
        for pulling, pdata in pulling_lista:
            pozo_referencia = pulling_asignaciones[pulling][-1][0] if pulling_asignaciones[pulling] else pdata["pozo"]
            candidatos = []
            for pozo in no_asignados:
                coef, dist = calcular_coeficiente(pozo_referencia, pozo)
                candidatos.append((pozo, coef, dist))
            candidatos.sort(key=lambda x: (-x[1], x[2]))
            if candidatos:
                mejor_candidato = candidatos[0]
                pulling_asignaciones[pulling].append(mejor_candidato)
                pozos_ocupados.add(mejor_candidato[0])
                no_asignados.remove(mejor_candidato[0])
            else:
                flash(f"⚠️ No hay pozos disponibles para asignar como {nivel} en {pulling}.")
        return pulling_asignaciones

    # ----------------------
    # Asignaciones para N+1, N+2 y N+3
    # ----------------------
    pulling_asignaciones = {pulling: [] for pulling, _ in pulling_lista}
    pulling_asignaciones = asignar_pozos(pulling_asignaciones, "N+1")
    pulling_asignaciones = asignar_pozos(pulling_asignaciones, "N+2")
    pulling_asignaciones = asignar_pozos(pulling_asignaciones, "N+3")

    # ----------------------
    # Construcción de la Matriz de Prioridad
    # ----------------------
    matriz_prioridad = []
    for pulling, pdata in pulling_lista:
        pozo_actual = pdata["pozo"]
        registro_actual = df[df["POZO"] == pozo_actual].iloc[0]
        neta_actual = registro_actual["NETA [M3/D]"]
        tiempo_restante = pdata["tiempo_restante"]

        seleccionados = pulling_asignaciones.get(pulling, [])[:3]
        while len(seleccionados) < 3:
            seleccionados.append(("N/A", 1, 1))

        coeficiente_actual = neta_actual / tiempo_restante if tiempo_restante > 0 else 0

        def get_neta(pozo):
            if pozo == "N/A":
                return 0
            row = df[df["POZO"] == pozo]
            return row["NETA [M3/D]"].iloc[0] if not row.empty else 0

        pozo_n1, coef_n1, dist_n1 = seleccionados[0]
        neta_n1 = get_neta(pozo_n1)
        registro_n1 = df[df["POZO"] == pozo_n1]
        tiempo_planificado_n1 = registro_n1["TIEMPO PLANIFICADO"].iloc[0] if not registro_n1.empty else 1
        denom_n1 = (0.5 * dist_n1) + tiempo_planificado_n1
        coeficiente_n1 = neta_n1 / denom_n1 if denom_n1 != 0 else 0

        pozo_n2, coef_n2, dist_n2 = seleccionados[1]
        neta_n2 = get_neta(pozo_n2)
        
        pozo_n3, coef_n3, dist_n3 = seleccionados[2]
        neta_n3 = get_neta(pozo_n3)

        matriz_prioridad.append([
            pulling,
            pozo_actual,
            neta_actual,
            tiempo_restante,
            pozo_n1,
            neta_n1,
            coef_n1,
            dist_n1,
            pozo_n2,
            neta_n2,
            coef_n2,
            dist_n2,
            pozo_n3,
            neta_n3,
            coef_n3,
            dist_n3
        ])

    columns = [
        "Pulling", "Pozo Actual", "Neta Actual", "Tiempo Restante (h)",
        "N+1", "Neta N+1", "Coeficiente N+1", "Distancia N+1 (km)",
        "N+2", "Neta N+2", "Coeficiente N+2", "Distancia N+2 (km)",
        "N+3", "Neta N+3", "Coeficiente N+3", "Distancia N+3 (km)"
    ]
    df_prioridad = pd.DataFrame(matriz_prioridad, columns=columns)

    df_styled = (df_prioridad.style
                 .hide_index()
                 .set_properties(**{"text-align": "center", "white-space": "nowrap"})
                 .format(precision=2)
                 .set_table_styles([
                     {"selector": "th", "props": [("background-color", "#f8f9fa"), 
                                                    ("color", "#333"), 
                                                    ("font-weight", "bold"), 
                                                    ("text-align", "center")]},
                     {"selector": "td", "props": [("padding", "8px")]},
                     {"selector": "tbody tr:nth-child(even)", "props": [("background-color", "#f2f2f2")]}
                 ]))
    table_html = df_styled.render()
    flash("Proceso de asignación completado.")
    return render_template("assign_result.html", table=table_html)

if __name__ == "__main__":
    app.run(debug=True)



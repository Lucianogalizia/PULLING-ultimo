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
      1. Normaliza los nombres de las columnas.
      2. Ordena de mayor a menor por "Pérdida [m3/d]" y obtiene un preview (20 filas).
      3. Filtra las filas donde "Plan [Si/No]" sea 1.
      4. Descarta filas en las que la celda "OBSERVACIONES" esté pintada de rojo.
      5. Elimina filas con valores nulos, vacíos o 0 en columnas críticas.
      6. Elimina filas cuyo valor en la columna "EQUIPO" contenga palabras no deseadas.
      7. (En lugar de convertir X e Y) Lee un Excel de coordenadas y hace un merge por "POZO"
         para obtener las columnas GEO_LATITUDE y GEO_LONGITUDE.
      8. Si algún pozo no se encuentra en el Excel de coordenadas, se avisa al usuario.
      9. Se conservan y renombran las columnas requeridas, y se agregan PROD_DT y RUBRO.
    """
    # --- Leer el Excel principal (hoja "dataset") ---
    wb = load_workbook(file_path, data_only=True)
    if "dataset" not in wb.sheetnames:
        raise ValueError("La hoja 'dataset' no se encontró en el archivo.")
    ws = wb["dataset"]

    # Extraer encabezados y normalizarlos
    header = []
    col_map = {}
    for idx, cell in enumerate(ws[1]):
        val = cell.value if cell.value is not None else ""
        norm = normalize_text(val)
        header.append(val)
        col_map[idx] = norm

    # Identificar la columna "OBSERVACIONES" (buscando "observac")
    observaciones_idx = None
    for idx, col_name in col_map.items():
        if "observac" in col_name:
            observaciones_idx = idx
            break

    # Recopilar filas (descartando las que tienen "OBSERVACIONES" en rojo)
    data = []
    for row in ws.iter_rows(min_row=2, values_only=False):
        if observaciones_idx is not None:
            cell_obs = row[observaciones_idx]
            red_flag = False
            if cell_obs.font and cell_obs.font.color and cell_obs.font.color.rgb:
                if str(cell_obs.font.color.rgb).upper() == "FFFF0000":
                    red_flag = True
            if red_flag:
                continue  # descartar fila
        row_data = {}
        for idx, cell in enumerate(row):
            key = header[idx]
            row_data[key] = cell.value
        data.append(row_data)
    
    # Crear DataFrame del Excel principal
    df_main = pd.DataFrame(data)

    # Normalizar nombres de columna y renombrar según lo esperado
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
    if "Pérdida [m3/d]" in df_main.columns:
        df_main["Pérdida [m3/d]"] = pd.to_numeric(df_main["Pérdida [m3/d]"], errors="coerce")
        df_main.sort_values(by="Pérdida [m3/d]", ascending=False, inplace=True)
    preview_df = df_main.head(20)

    if "Plan [Si/No]" in df_main.columns:
        df_main = df_main[df_main["Plan [Si/No]"] == 1]

    cols_criticas = ["Activo", "POZO", "X", "Y", "Pérdida [m3/d]", "Plan [Si/No]", "Plan [Hs/INT]", "EQUIPO"]
    for col in cols_criticas:
        if col in df_main.columns:
            df_main = df_main[df_main[col].notnull()]
            df_main = df_main[df_main[col] != 0]

    if "EQUIPO" in df_main.columns:
        patrones = ["fb", "pesado", "z inyector", "z recupero"]
        def no_contiene(valor):
            if not isinstance(valor, str):
                return True
            valor_norm = normalize_text(valor)
            return not any(pat in valor_norm for pat in patrones)
        df_main = df_main[df_main["EQUIPO"].apply(no_contiene)]

    # --- Merge con el Excel de coordenadas ---
    # Leer el Excel de coordenadas (asegúrate de que 'coordenadas.xlsx' esté en tu proyecto)
    df_coords = pd.read_excel("coordenadas.xlsx")
    # Nos quedamos solo con las columnas necesarias
    df_coords = df_coords[["POZO", "GEO_LATITUDE", "GEO_LONGITUDE"]]

    # Reemplazar comas por puntos y convertir a float
    df_coords["GEO_LATITUDE"] = df_coords["GEO_LATITUDE"].astype(str).str.replace(",", ".").astype(float)
    df_coords["GEO_LONGITUDE"] = df_coords["GEO_LONGITUDE"].astype(str).str.replace(",", ".").astype(float)
    
    # Realizar el merge por la columna POZO (left join)
    df_merged = df_main.merge(df_coords, on="POZO", how="left")

    # Verificar si faltan coordenadas y avisar al usuario
    missing_pozos = df_merged[
        df_merged["GEO_LATITUDE"].isnull() | df_merged["GEO_LONGITUDE"].isnull()
    ]["POZO"].unique()
    if len(missing_pozos) > 0:
        flash(
            f"Atención: No se encontraron coordenadas para los siguientes pozos: {', '.join(missing_pozos)}"
        )
        # Opcional: eliminar filas sin coordenadas para evitar errores posteriores
        df_merged = df_merged.dropna(subset=["GEO_LATITUDE", "GEO_LONGITUDE"])

    # --- Selección y renombrado de columnas finales ---
    # Conservamos las columnas que necesitamos del DataFrame fusionado
    columnas_requeridas = ["Activo", "POZO", "Pérdida [m3/d]", "Plan [Hs/INT]", "Batería"]
    df_merged = df_merged[[col for col in columnas_requeridas if col in df_merged.columns] + ["GEO_LATITUDE", "GEO_LONGITUDE"]]

    # Renombrar columnas para el output final
    df_merged.rename(columns={
        "Activo": "ZONA",
        "Pérdida [m3/d]": "NETA [M3/D]",
        "Plan [Hs/INT]": "TIEMPO PLANIFICADO",
        "Batería": "BATERÍA"
    }, inplace=True)

    # Agregar columnas fijas: fecha de producción y rubro
    df_merged["PROD_DT"] = datetime.date.today().strftime("%Y-%m-%d")
    df_merged["RUBRO"] = "ESPERA DE TRACTOR"

    # Reordenar columnas según lo solicitado
    orden_final = ["POZO", "NETA [M3/D]", "PROD_DT", "RUBRO", "GEO_LATITUDE", "GEO_LONGITUDE", "BATERÍA", "ZONA", "TIEMPO PLANIFICADO"]
    df_merged = df_merged[[col for col in orden_final if col in df_merged.columns]]

    return df_merged, preview_df

# =============================================================================
# Rutas de la Aplicación Flask
# =============================================================================
@app.route("/")
def index():
    return redirect(url_for("upload_file"))

@app.route("/upload", methods=["GET", "POST"])
def upload_file():
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
            df_clean, preview_df = process_excel(filepath)
        except Exception as e:
            flash(f"Error al procesar el Excel: {e}")
            return redirect(request.url)

        data_store["df"] = df_clean
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
    """
    Permite al usuario seleccionar los pozos para pulling y
    especificar manualmente el valor de NETA para cada uno.
    """
    try:
        df_wells = pd.read_excel("coordenadas.xlsx")
        
        # Eliminar filas donde la columna POZO sea NaN
        df_wells.dropna(subset=["POZO"], inplace=True)
        
        # Convertir todos los valores de la columna POZO a string
        df_wells["POZO"] = df_wells["POZO"].astype(str)
        
        # Ahora sí, podemos ordenar la lista sin errores
        pulling_wells = sorted(df_wells["POZO"].unique().tolist())
    except Exception as e:
        flash(f"Error al leer el archivo de pozos: {e}")
        return redirect(url_for("filter_zonas"))
    
    # Se obtiene la cantidad de pullings elegida previamente (desde /filter)
    pulling_count = data_store.get("pulling_count", 3)
    
    if request.method == "POST":
        pulling_data = {}
        seleccionados = []
        for i in range(1, pulling_count + 1):
            # Obtener el pozo seleccionado
            pozo = request.form.get(f"pulling_pozo_{i}")
            # Obtener el valor de NETA ingresado por el usuario
            neta = request.form.get(f"neta_{i}")
            if neta is None or neta.strip() == "":
                flash(f"Debes ingresar un valor de NETA para Pulling {i}.")
                return redirect(request.url)
            try:
                # Reemplaza comas por puntos y convierte a float
                neta_val = float(neta.replace(",", "."))
            except Exception as e:
                flash(f"Valor de NETA inválido para Pulling {i}: {neta}")
                return redirect(request.url)
            
            pulling_data[f"Pulling {i}"] = {
                "pozo": pozo,
                "neta": neta_val
            }
            seleccionados.append(pozo)
        
        # Validar que no se repita el mismo pozo en más de un pulling
        if len(seleccionados) != len(set(seleccionados)):
            flash("Error: No puedes seleccionar el mismo pozo para más de un pulling.")
            return redirect(request.url)
        
        data_store["pulling_data"] = pulling_data
        flash("Selección de Pulling confirmada.")
        return redirect(url_for("assign"))
    
    # Para el método GET: generar el formulario de selección
    # Construir las opciones para el dropdown a partir del Excel externo
    select_options = ""
    for pozo in pulling_wells:
        select_options += f'<option value="{pozo}">{pozo}</option>'
    
    # Generar el HTML para cada bloque de pulling (dropdown y input para NETA)
    form_html = ""
    for i in range(1, pulling_count + 1):
        form_html += f"""
        <h4>Pulling {i}</h4>
        <div class="mb-2">
          <label>Pozo para Pulling {i}:</label>
          <select name="pulling_pozo_{i}" class="form-select w-50">
            {select_options}
          </select>
        </div>
        <div class="mb-2">
          <label>NETA para Pulling {i}:</label>
          <input type="number" name="neta_{i}" class="form-control w-50" step="0.01" required>
        </div>
        <hr>
        """
    
    return render_template("select_pulling.html", form_html=form_html)

# Nueva ruta para la asignación
@app.route("/assign", methods=["GET"])
def assign():
    if "pulling_data" not in data_store:
        flash("Debes seleccionar los pozos para pulling primero.")
        return redirect(url_for("select_pulling"))

    df = data_store["df"]
    pulling_data = data_store["pulling_data"]

    # Aquí va tu lógica de asignación. Por ejemplo:
    matriz_prioridad = []
    pozos_ocupados = set()
    pulling_lista = list(pulling_data.items())

    def calcular_coeficiente(pozo_referencia, pozo_candidato):
        registro_ref = df[df["POZO"] == pozo_referencia].iloc[0]
        registro_cand = df[df["POZO"] == pozo_candidato].iloc[0]
        distancia = geodesic(
            (registro_ref["GEO_LATITUDE"], registro_ref["GEO_LONGITUDE"]),
            (registro_cand["GEO_LATITUDE"], registro_cand["GEO_LONGITUDE"])
        ).kilometers
        neta = registro_cand["NETA [M3/D]"]
        tiempo_plan = registro_cand["TIEMPO PLANIFICADO"]
        coeficiente = neta / (tiempo_plan + (distancia * 0.5)) if (tiempo_plan + (distancia * 0.5)) != 0 else 0
        return coeficiente, distancia

    def asignar_pozos(pulling_asignaciones, nivel):
        no_asignados = [p for p in data_store["pozos_disponibles"] if p not in pozos_ocupados]
        for pulling, data in pulling_lista:
            pozo_referencia = pulling_asignaciones[pulling][-1][0] if pulling_asignaciones[pulling] else data["pozo"]
            candidatos = []
            for pozo in no_asignados:
                coef, dist = calcular_coeficiente(pozo_referencia, pozo)
                candidatos.append((pozo, coef, dist))
            candidatos.sort(key=lambda x: (-x[1], x[2]))
            if candidatos:
                mejor_candidato = candidatos[0]
                pulling_asignaciones[pulling].append(mejor_candidato)
                pozos_ocupados.add(mejor_candidato[0])
                if mejor_candidato[0] in no_asignados:
                    no_asignados.remove(mejor_candidato[0])
            else:
                flash(f"⚠️ No hay pozos disponibles para asignar como {nivel} en {pulling}.")
        return pulling_asignaciones

    pulling_asignaciones = {pulling: [] for pulling, _ in pulling_lista}
    pulling_asignaciones = asignar_pozos(pulling_asignaciones, "N+1")
    pulling_asignaciones = asignar_pozos(pulling_asignaciones, "N+2")
    pulling_asignaciones = asignar_pozos(pulling_asignaciones, "N+3")

    for pulling, data in pulling_lista:
        pozo_actual = data["pozo"]
        registro_actual = df[df["POZO"] == pozo_actual].iloc[0]
        neta_actual = registro_actual["NETA [M3/D]"]
        tiempo_restante = data["tiempo_restante"]
        seleccionados = pulling_asignaciones.get(pulling, [])[:3]
        while len(seleccionados) < 3:
            seleccionados.append(("N/A", 1, 1))
        coeficiente_actual = neta_actual / tiempo_restante if tiempo_restante > 0 else 0
        distancia_n1 = seleccionados[0][2]
        registro_n1 = df[df["POZO"] == seleccionados[0][0]]
        if not registro_n1.empty:
            tiempo_planificado_n1 = registro_n1["TIEMPO PLANIFICADO"].iloc[0]
            neta_n1 = registro_n1["NETA [M3/D]"].iloc[0]
        else:
            tiempo_planificado_n1 = 1
            neta_n1 = 1
        coeficiente_n1 = neta_n1 / ((0.5 * distancia_n1) + tiempo_planificado_n1)

        if coeficiente_actual < coeficiente_n1:
            recomendacion = "Abandonar pozo actual y moverse al N+1"
        else:
            recomendacion = "Continuar en pozo actual"

        matriz_prioridad.append([
            pulling,
            pozo_actual,
            neta_actual,
            tiempo_restante,
            seleccionados[0][0],
            seleccionados[0][1],
            seleccionados[0][2],
            seleccionados[1][0],
            seleccionados[1][1],
            seleccionados[1][2],
            seleccionados[2][0],
            seleccionados[2][1],
            seleccionados[2][2],
            recomendacion
        ])

    columns = [
        "Pulling", "Pozo Actual", "Neta Actual", "Tiempo Restante (h)",
        "N+1", "Coeficiente N+1", "Distancia N+1 (km)",
        "N+2", "Coeficiente N+2", "Distancia N+2 (km)",
        "N+3", "Coeficiente N+3", "Distancia N+3 (km)", "Recomendación"
    ]
    df_prioridad = pd.DataFrame(matriz_prioridad, columns=columns)

    def highlight_reco(val):
        if "Abandonar" in val:
            return "color: red; font-weight: bold;"
        else:
            return "color: green; font-weight: bold;"

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
                 ])
                 .applymap(highlight_reco, subset=["Recomendación"]))
    
    table_html = df_styled.render()
    flash("Proceso de asignación completado.")
    return render_template("assign_result.html", table=table_html)

if __name__ == "__main__":
    app.run(debug=True)

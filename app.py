# =============================================================================
# Importación de Librerías y Configuración Inicial
# =============================================================================
from flask import Flask, request, redirect, url_for, render_template, flash, jsonify
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
      4. Descarta filas en las que la celda "OBSERVACIONES" esté pintada de rojo (relleno rojo).
      5. Elimina filas con valores nulos, vacíos o 0 en columnas críticas.
      6. Elimina filas cuyo valor en la columna "EQUIPO" contenga palabras no deseadas.
      --> 6.1. **Normaliza la columna POZO** del Excel del usuario para que coincida con el Excel de coordenadas.
      7. (En lugar de convertir X e Y) Lee un Excel de coordenadas y hace un merge por "POZO"
         para obtener las columnas GEO_LATITUDE y GEO_LONGITUDE.
      8. Si algún pozo no se encuentra en el Excel de coordenadas, se avisa al usuario.
      9. Se conservan y renombran las columnas requeridas, y se agregan PROD_DT y RUBRO.
    """
    import pandas as pd
    import datetime
    import os
    import re
    import unicodedata
    from openpyxl import load_workbook
    from geopy.distance import geodesic
    from difflib import SequenceMatcher

    # ---------------------------
    # Funciones Auxiliares
    # ---------------------------
    def normalize_text(text):
        if not isinstance(text, str):
            return text
        text = text.strip().lower()
        text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
        text = re.sub(r'\s+', ' ', text)
        return text

    def extract_number(s):
        match = re.search(r'(\d+)', s)
        if match:
            return match.group(1)
        return ""

    def extract_letters(s):
        # Extrae las letras y las concatena en mayúsculas.
        letters = re.findall(r'[A-Z]+', s.upper())
        return "".join(letters)

    def custom_normalize_pozo(user_pozo, coord_list, letter_threshold=0.5):
        """
        Dado un valor de pozo del usuario, filtra candidatos de la lista del Excel de coordenadas
        que tengan exactamente el mismo número; luego elige el que tenga mayor similitud en la parte
        de letras. Si no hay candidatos, retorna el valor original.
        """
        if not isinstance(user_pozo, str):
            return user_pozo
        user_pozo = user_pozo.strip()
        user_number = extract_number(user_pozo)
        user_letters = extract_letters(user_pozo)
        
        # Filtrar candidatos con el mismo número
        candidates = []
        for cand in coord_list:
            cand = cand.strip()
            cand_number = extract_number(cand)
            if cand_number == user_number and user_number != "":
                candidates.append(cand)
        if not candidates:
            return user_pozo
        if len(candidates) == 1:
            return candidates[0]
        
        best_candidate = None
        best_ratio = 0
        for cand in candidates:
            cand_letters = extract_letters(cand)
            ratio = SequenceMatcher(None, user_letters, cand_letters).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_candidate = cand
        return best_candidate if best_ratio >= letter_threshold else user_pozo

    # ---------------------------
    # Lectura del Excel principal con openpyxl
    # ---------------------------
    wb = load_workbook(file_path, data_only=True)
    if "dataset" not in wb.sheetnames:
        raise ValueError("La hoja 'dataset' no se encontró en el archivo.")
    ws = wb["dataset"]

    header = []
    col_map = {}
    # Agregamos un campo extra "CELESTE" para marcar si la celda de POZO está pintada de celeste.
    for idx, cell in enumerate(ws[1]):
        val = cell.value if cell.value is not None else ""
        header.append(val)
        col_map[idx] = normalize_text(val)

    observaciones_idx = None
    pozo_idx = None
    equipo_idx = None
    for idx, col_name in col_map.items():
        if "observac" in col_name:
            observaciones_idx = idx
        if "pozo" in col_name:
            pozo_idx = idx
        if "equipo" in col_name:
            equipo_idx = idx

    data = []
    # En lugar de guardar directamente los nombres de pozos celestes, se guarda un indicador en cada fila
    for row in ws.iter_rows(min_row=2, values_only=False):
        row_data = {}
        # Inicialmente no tiene el indicador "CELESTE"
        row_data["CELESTE"] = False

        # 1) Descartar si OBSERVACIONES está pintada de rojo
        if observaciones_idx is not None:
            cell_obs = row[observaciones_idx]
            if cell_obs.fill and cell_obs.fill.fgColor:
                fg_obs = cell_obs.fill.fgColor
                if fg_obs.type == "rgb" and fg_obs.rgb and fg_obs.rgb.upper() == "FFFF0000":
                    continue

        # 2) Descartar si EQUIPO contiene "B2" o "B3"
        if equipo_idx is not None:
            cell_equipo = row[equipo_idx]
            if cell_equipo.value:
                valor_equipo = str(cell_equipo.value).lower()
                if "b2" in valor_equipo or "b3" in valor_equipo:
                    continue

        # 3) Detectar pozos pintados de celeste (relleno "FF00FFFF")
        if pozo_idx is not None:
            cell_pozo = row[pozo_idx]
            if cell_pozo.fill and cell_pozo.fill.fgColor:
                fg_pozo = cell_pozo.fill.fgColor
                if fg_pozo.type == "rgb" and fg_pozo.rgb and fg_pozo.rgb.upper() == "FF00FFFF":
                    # Marcamos en esta fila que el pozo era celeste
                    row_data["CELESTE"] = True

        # 4) Construir el diccionario para la fila usando la fila de encabezados
        for i, cell in enumerate(row):
            key = header[i]
            row_data[key] = cell.value
        data.append(row_data)

    df_main = pd.DataFrame(data)

    # ---------------------------
    # Normalizar nombres de columna y renombrar según lo esperado
    # ---------------------------
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

    # ---------------------------
    # Procesamiento de datos adicionales
    # ---------------------------
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

    # ----------------------------------------------------
    # **NUEVA PARTE: Normalización de la columna POZO**
    # ----------------------------------------------------
    # Cargar el Excel de coordenadas (se asume que tiene la columna "POZO")
    df_coords = pd.read_excel("coordenadas.xlsx", engine="openpyxl")
    # Extraer la lista de pozos reales del Excel de coordenadas
    lista_pozos_coords = df_coords["POZO"].dropna().astype(str).tolist()
    
    # Creamos una columna temporal para facilitar un merge exacto:
    df_main["POZO_TMP"] = df_main["POZO"].astype(str).str.strip().str.upper()
    df_coords["POZO_TMP"] = df_coords["POZO"].astype(str).str.strip().str.upper()
    
    # Merge exacto basado en POZO_TMP
    df_main = pd.merge(
        df_main,
        df_coords[["POZO", "POZO_TMP"]],
        on="POZO_TMP",
        how="left",
        suffixes=("", "_coord")
    )
    
    # Para registros sin match exacto, aplicar normalización personalizada.
    def apply_normalization(row):
        if pd.isnull(row.get("POZO_coord")):
            return custom_normalize_pozo(row["POZO"], lista_pozos_coords, letter_threshold=0.5)
        return row["POZO_coord"]
    
    df_main["POZO_NORMALIZADO"] = df_main.apply(apply_normalization, axis=1)
    
    # Actualizar la columna POZO con el valor normalizado y descartar columnas temporales
    df_main["POZO"] = df_main["POZO_NORMALIZADO"]
    df_main.drop(columns=["POZO_TMP", "POZO_coord", "POZO_NORMALIZADO"], inplace=True)
    # ----------------------------------------------------
    
    # ----------------------------------------------------
    # Reconstruir la lista de pozos celestes: usar el indicador "CELESTE" de cada fila
    pozos_celestes = df_main[df_main["CELESTE"] == True]["POZO"].unique().tolist()
    # ----------------------------------------------------
    
    # --- Merge con el Excel de coordenadas para obtener GEO_LATITUDE y GEO_LONGITUDE ---
    df_coords["GEO_LATITUDE"] = df_coords["GEO_LATITUDE"].astype(str).str.replace(",", ".").astype(float)
    df_coords["GEO_LONGITUDE"] = df_coords["GEO_LONGITUDE"].astype(str).str.replace(",", ".").astype(float)
    df_merged = df_main.merge(df_coords[["POZO", "GEO_LATITUDE", "GEO_LONGITUDE"]], on="POZO", how="left")
    
    missing_pozos = df_merged[
        df_merged["GEO_LATITUDE"].isnull() | df_merged["GEO_LONGITUDE"].isnull()
    ]["POZO"].unique()
    if len(missing_pozos) > 0:
        flash(f"Atención: No se encontraron coordenadas para los siguientes pozos: {', '.join(missing_pozos)}")
        df_merged = df_merged.dropna(subset=["GEO_LATITUDE", "GEO_LONGITUDE"])
    
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




 
# ========================
# Rutas de la Aplicación
# ========================

@app.route("/")
def index():
    return redirect(url_for("upload_file"))

@app.route("/upload", methods=["GET", "POST"])
def upload_file():
    """
    GET: muestra formulario de subida.
    POST: guarda el Excel, encola la tarea y devuelve inmediatamente la pantalla de 'pendiente'.
    """
    if request.method == "POST":
        # 1) Validaciones básicas
        if "excel_file" not in request.files:
            flash("No se encontró el archivo en la solicitud.")
            return redirect(request.url)

        file = request.files["excel_file"]
        if file.filename == "":
            flash("No se seleccionó ningún archivo.")
            return redirect(request.url)

        # 2) Guardar el archivo rápidamente
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)

        # 3) Import dinámico y encolar la tarea
        from celery_worker import process_excel_task
        task = process_excel_task.delay(filepath)

        # 4) Responder _inmediatamente_ con la plantilla de espera
        return render_template("upload_pending.html", task_id=task.id), 202

    # Para GET, simplemente renderizamos el formulario
    return render_template("upload.html")


@app.route("/status/<task_id>", methods=["GET"])
def task_status(task_id):
    """
    Consulta el estado de la tarea Celery y devuelve:
      - 202 + JSON {'state': ...} si sigue PENDING/STARTED
      - 200 + render_template(...) si SUCCESS
      - 500 + JSON {'state','error'} en FAILURE
    """
    from celery_worker import celery
    import pandas as pd

    res = celery.AsyncResult(task_id)
    state = res.state

    if state in ('PENDING', 'STARTED'):
        return jsonify({'state': state}), 202

    if state == 'SUCCESS':
        result = res.result or {}
        # Guardamos pozos celestes
        data_store['pozos_celestes'] = result.get('pozos_celestes', [])
        # Reconstruimos y guardamos el DataFrame completo
        records = result.get('data_records', [])
        df_clean = pd.DataFrame.from_records(records)
        data_store['df'] = df_clean
        # Renderizamos la plantilla final con el preview
        return render_template("upload_success.html", preview=result.get('preview', ''))

    # En caso de error
    return jsonify({
        'state': state,
        'error': str(res.result)
    }), 500 
       
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
    
    # Recuperamos la lista de pozos pintados de celeste
    pozos_celestes = data_store.get("celeste_pozos", [])

    if request.method == "POST":
        # Aquí se recogen los pozos seleccionados en el formulario
        pulling_data = {}
        seleccionados = []
        for i in range(1, pulling_count + 1):
            pozo = request.form.get(f"pulling_pozo_{i}")
            pulling_data[f"Pulling {i}"] = {
                "pozo": pozo,
                "tiempo_restante": 0.0
            }
            seleccionados.append(pozo)

        # Validaciones, etc.
        if len(seleccionados) != len(set(seleccionados)):
            flash("Error: No puedes seleccionar el mismo pozo para más de un pulling.")
            return redirect(request.url)

        data_store["pulling_data"] = pulling_data

        # Quitamos los pozos seleccionados de la lista total
        todos_pozos = sorted(df_filtrado["POZO"].unique().tolist())
        data_store["pozos_disponibles"] = [p for p in todos_pozos if p not in seleccionados]

        flash("Selección de Pulling confirmada.")
        return redirect(url_for("assign"))

    # Construimos el formulario en HTML dinámicamente,
    # usando la lista de pozos disponibles y pre-seleccionando los celestes.
    form_html = ""
    for i in range(1, pulling_count + 1):
        # Determinamos cuál pozo celeste (si existe) le toca a este Pulling
        # i.e., si i=1 -> pozos_celestes[0], i=2 -> pozos_celestes[1], etc.
        default_pozo = None
        if (i - 1) < len(pozos_celestes):
            default_pozo = pozos_celestes[i - 1]

        # Construimos las opciones del <select>
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
 
# Nueva ruta para la asignación
@app.route("/assign", methods=["GET"])
def assign():
    if "pulling_data" not in data_store:
        flash("Debes seleccionar los pozos para pulling primero.")
        return redirect(url_for("select_pulling"))
 
    df = data_store["df"]
    pulling_data = data_store["pulling_data"]
 
    # Lista de tuplas (pulling_name, {"pozo": <>, "tiempo_restante": <>})
    pulling_lista = list(pulling_data.items())
    pozos_ocupados = set()
 
    # ----------------------
    # 1. Funciones auxiliares
    # ----------------------
    def calcular_coeficiente(pozo_referencia, pozo_candidato):
        """
        Calcula coeficiente = NETA / (TIEMPO_PLAN + 0.5*distancia)
        Retorna (coeficiente, distancia).
        """
        registro_ref = df[df["POZO"] == pozo_referencia].iloc[0]
        registro_cand = df[df["POZO"] == pozo_candidato].iloc[0]
        distancia = geodesic(
            (registro_ref["GEO_LATITUDE"], registro_ref["GEO_LONGITUDE"]),
            (registro_cand["GEO_LATITUDE"], registro_cand["GEO_LONGITUDE"])
        ).kilometers
        neta = registro_cand["NETA [M3/D]"]
        tiempo_plan = registro_cand["TIEMPO PLANIFICADO"]
        denominador = (tiempo_plan + 0.5 * distancia)
        coeficiente = neta / denominador if denominador != 0 else 0
        return coeficiente, distancia
 
    def asignar_pozos(pulling_asignaciones, nivel):
        """
        Asigna pozos para cada pulling según el nivel (N+1, N+2, N+3).
        Toma los pozos no asignados y selecciona el "mejor candidato" según el coeficiente.
        """
        no_asignados = [p for p in data_store["pozos_disponibles"] if p not in pozos_ocupados]
        for pulling, pdata in pulling_lista:
            # Si ya tiene asignados, usamos el último pozo; sino usamos el pozo original
            pozo_referencia = pulling_asignaciones[pulling][-1][0] if pulling_asignaciones[pulling] else pdata["pozo"]
            candidatos = []
            for pozo in no_asignados:
                coef, dist = calcular_coeficiente(pozo_referencia, pozo)
                candidatos.append((pozo, coef, dist))
            # Ordenar desc. por coef y asc. por distancia
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
    # 2. Asignaciones (N+1, N+2, N+3)
    # ----------------------
    pulling_asignaciones = {pulling: [] for pulling, _ in pulling_lista}
    pulling_asignaciones = asignar_pozos(pulling_asignaciones, "N+1")
    pulling_asignaciones = asignar_pozos(pulling_asignaciones, "N+2")
    pulling_asignaciones = asignar_pozos(pulling_asignaciones, "N+3")
 
    # ----------------------
    # 3. Construcción de la Matriz de Prioridad
    # ----------------------
    matriz_prioridad = []
 
    for pulling, pdata in pulling_lista:
        pozo_actual = pdata["pozo"]
        registro_actual = df[df["POZO"] == pozo_actual].iloc[0]
        neta_actual = registro_actual["NETA [M3/D]"]
        tiempo_restante = pdata["tiempo_restante"]
 
        # Obtenemos hasta 3 pozos (N+1, N+2, N+3)
        seleccionados = pulling_asignaciones.get(pulling, [])[:3]
        # Si no hay suficientes pozos, rellenamos con N/A
        while len(seleccionados) < 3:
            seleccionados.append(("N/A", 1, 1))
 
        # Coeficiente del pozo actual
        coeficiente_actual = 0
        if tiempo_restante > 0:
            coeficiente_actual = neta_actual / tiempo_restante
 
        # ----------------------
        # 3.1 Obtener info de N+1, N+2, N+3 (pozo, neta, coef, dist)
        # ----------------------
        # Función interna para buscar NETA en df
        def get_neta(pozo):
            if pozo == "N/A":
                return 0
            row = df[df["POZO"] == pozo]
            if not row.empty:
                return row["NETA [M3/D]"].iloc[0]
            return 0
 
        # N+1
        pozo_n1, coef_n1, dist_n1 = seleccionados[0]
        neta_n1 = get_neta(pozo_n1)
        # Se usa para recomendación
        registro_n1 = df[df["POZO"] == pozo_n1]
        if not registro_n1.empty:
            tiempo_planificado_n1 = registro_n1["TIEMPO PLANIFICADO"].iloc[0]
        else:
            tiempo_planificado_n1 = 1
        # Coef N+1 para recomendación
        denom_n1 = (0.5 * dist_n1) + tiempo_planificado_n1
        coeficiente_n1 = neta_n1 / denom_n1 if denom_n1 != 0 else 0
 
        # N+2
        pozo_n2, coef_n2, dist_n2 = seleccionados[1]
        neta_n2 = get_neta(pozo_n2)
 
        # N+3
        pozo_n3, coef_n3, dist_n3 = seleccionados[2]
        neta_n3 = get_neta(pozo_n3)
 
        # ----------------------
        # 3.2 Generar Recomendación
        # ----------------------
        #if coeficiente_actual < coeficiente_n1:
            #recomendacion = "Abandonar pozo actual y moverse al N+1"
        #else:
            #recomendacion = "Continuar en pozo actual"
 
        # ----------------------
        # 3.3 Agregar fila a la matriz
        # ----------------------
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
            #recomendacion
        ])
 
    # ----------------------
    # 4. Crear DataFrame con las nuevas columnas
    # ----------------------
    columns = [
        "Pulling",
        "Pozo Actual",
        "Neta Actual",
        "Tiempo Restante (h)",
        "N+1",
        "Neta N+1",
        "Coeficiente N+1",
        "Distancia N+1 (km)",
        "N+2",
        "Neta N+2",
        "Coeficiente N+2",
        "Distancia N+2 (km)",
        "N+3",
        "Neta N+3",
        "Coeficiente N+3",
        "Distancia N+3 (km)"
        #"Recomendación"
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
                 #.applymap(highlight_reco, subset=["Recomendación"])
                )
    table_html = df_styled.render()
    flash("Proceso de asignación completado.")
    return render_template("assign_result.html", table=table_html)
 
if __name__ == "__main__":
    app.run(debug=True)

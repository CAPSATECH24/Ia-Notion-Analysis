# Import necessary libraries
import streamlit as st
import pandas as pd
import google.generativeai as genai
import os
import time
import json
from io import StringIO
import datetime
import re # Import regular expression library
import traceback # Import traceback for detailed error logging

# --- Streamlit Page Configuration ---
st.set_page_config(page_title="Analizador de Servicios con Gemini", layout="wide")
st.title("🛰️ Analizador de Historial de Servicios GPS con IA")
st.markdown("""
Esta aplicación utiliza la API de Gemini para analizar la columna 'DESCRIPTION' de tu historial de servicios,
extraer los componentes instalados/desinstalados y determinar el estado actual de cada dispositivo (IMEI) por cliente.
**Ahora puedes filtrar por cliente, ajustar el tamaño del lote y ver el log de depuración en tiempo real.**

""")

# --- Enlace a Notion ---
st.markdown(
    "Fuente de datos original: "
    "[Vista Notion](https://www.notion.so/4bc5d60d53494515a3b219ac9b718ac2?v=daa6506e3f1e43fa9be3d74420ec1b27)",
    help="Haz clic para ir a la vista de Notion desde donde se exporta el CSV."
)

# --- Initialize Session State for Log and Data ---
default_values = {
    'log_string': "Log de procesamiento aparecerá aquí...\n",
    'processing_complete': False,
    'events_df': None,
    'current_state_df': None,
    'df_loaded': None,
    'file_name': None,
    'column_options': [],
    'min_date': None,
    'max_date': None,
    'api_key': os.environ.get("GEMINI_API_KEY", ""),
    'imei_col': None,
    'desc_col': None,
    'date_col': None,
    'client_col': None,
    'start_date': None,
    'end_date': None,
    'batch_size': 25,
    'selected_clients_list': ["-- TODOS --"]
}
for key, value in default_values.items():
    if key not in st.session_state:
        st.session_state[key] = value


# --- Component and Action Definitions (Important for Prompt and Mapping) ---
COMPONENTES_ESTANDAR = [
    "GPS", "Paro de Motor", "Boton Panico", "Antena GPS", "Antena GPRS",
    "Arnés", "Sensor Puerta", "Sensor Combustible", "Sensor Temperatura",
    "Sensor Desenganche", "Sensor Impacto", "Sensor Jamming", "Power Hub",
    "iButton", "Chapa Electronica", "Bocina",
    "Microfono", "Telemetria", "CAN Bus", "Camara", "Modulo Voz", "Display",
    "Sensor DMS", "Sensor Fatiga", "GPS Señuelo",
    "Kit ADAS/DMS", "GPS Portatil", "Bateria Respaldo", "Sirena", "MDVR",
    "Relevador", "Teclado"
]
MAPEO_COMPONENTES = {
    # GPS y variantes
    "gps": "GPS", "dispositivo": "GPS", "equipo": "GPS", "localizador": "GPS", "unidad gps": "GPS", "equ": "GPS", "unidad": "GPS", "equipo gps": "GPS", "gtrack pro": "GPS", "gtrack-pro": "GPS", "gtrack": "GPS", "trace5": "GPS", "teltonika fmb920": "GPS", "teltonika fm3612": "GPS", "teltonika fmc920": "GPS", "teltonika fmc130": "GPS", "teltonika fmu125": "GPS", "teltonika fmu130": "GPS", "teltonika fmm130": "GPS", "teltonika fmb120": "GPS", "suntech st3300": "GPS", "suntech st4300": "GPS", "suntech st300": "GPS", "ruptela trace5": "GPS", "ruptela fm eco4 light": "GPS", "ruptela pro5 lite": "GPS", "ruptela hcv5": "GPS", "concox gt06n": "GPS", "concox gt06": "GPS", "queclink gv310lau": "GPS", "topflytech tlw1-4a/e": "GPS", "dk12": "GPS",
    "gps portatil": "GPS Portatil", "portatil": "GPS Portatil", "equipo portatil": "GPS Portatil", "gtrackflex": "GPS Portatil", "gtrack flex": "GPS Portatil", "sinotrack st-901": "GPS Portatil",
    "señuelo": "GPS Señuelo", "gps señuelo": "GPS Señuelo",
    # Paro Motor y variantes
    "paro motor": "Paro de Motor", "cortacorriente": "Paro de Motor", "corta corriente": "Paro de Motor", "corte de motor": "Paro de Motor", "bloqueo de motor": "Paro de Motor", "paro": "Paro de Motor", "paro de aceleracion": "Paro de Motor", "bloqueo de acelerador": "Paro de Motor", "corte": "Paro de Motor", "inst corte": "Paro de Motor",
    # Botón Pánico y variantes
    "boton de panico": "Boton Panico", "pánico": "Boton Panico", "panico": "Boton Panico", "botón pánico": "Boton Panico", "boton": "Boton Panico", "boton asistencia": "Boton Panico", "botón de asistencia": "Boton Panico",
    # Antenas
    "antena gps": "Antena GPS",
    "antena gprs": "Antena GPRS", "antena celular": "Antena GPRS",
    # Arnés (Generalmente ignorado, pero mapeado por si acaso)
    "arnes": "Arnés", "cableado": "Arnés", "arnés": "Arnés",
    # Sensores Puerta y variantes
    "sensor de puerta": "Sensor Puerta", "sensor puerta": "Sensor Puerta", "magnetico puerta": "Sensor Puerta", "sensor magnético": "Sensor Puerta", "sensor de apertura": "Sensor Puerta", "sensor de apertura de puerta": "Sensor Puerta", "sensor de puerta cableado": "Sensor Puerta", "sensor de puerta magnetico": "Sensor Puerta", "sensores de apertura": "Sensor Puerta",
    # Sensores Combustible y variantes
    "sensor de combustible": "Sensor Combustible", "sensor combustible": "Sensor Combustible", "medidor combustible": "Sensor Combustible", "sensor diesel": "Sensor Combustible", "barras de combustible": "Sensor Combustible", "barra de combustible": "Sensor Combustible", "barra": "Sensor Combustible", "barras": "Sensor Combustible", "td ble": "Sensor Combustible",
    # Sensores Temperatura y variantes
    "sensor de temperatura": "Sensor Temperatura", "sensor temperatura": "Sensor Temperatura", "termometro": "Sensor Temperatura", "sensor t°": "Sensor Temperatura", "sensor de temperatura bluetooth": "Sensor Temperatura", "sensor de temperatura cableado": "Sensor Temperatura", "sensor tipo temp": "Sensor Temperatura", "sensor bluetooth": "Sensor Temperatura", "eye sensor": "Sensor Temperatura", "temp sensor": "Sensor Temperatura", "ble sensor": "Sensor Temperatura", "sensor t": "Sensor Temperatura", "dallas": "Sensor Temperatura",
    # Otros Sensores
    "sensor de desenganche": "Sensor Desenganche", "sensor desenganche": "Sensor Desenganche", "sensor quinta rueda": "Sensor Desenganche",
    "sensor de impacto": "Sensor Impacto", "sensor impacto": "Sensor Impacto", "sensor colision": "Sensor Impacto", "sensor de colision": "Sensor Impacto",
    "sensor jamming": "Sensor Jamming", "detector jamming": "Sensor Jamming", "anti jamming": "Sensor Jamming", "detector de jamming": "Sensor Jamming",
    "sensor dms": "Sensor DMS",
    "sensor fatiga": "Sensor Fatiga", "sensor de fatiga": "Sensor Fatiga",
    # Power Hub y variantes
    "power hub": "Power Hub", "hub de energia": "Power Hub", "hub energia": "Power Hub", "powerhub": "Power Hub", "power lite": "Power Hub", "pw hub": "Power Hub", "phub": "Power Hub", "pwl": "Power Hub",
    # Batería
    "bateria respaldo": "Bateria Respaldo", "bateria de respaldo": "Bateria Respaldo", "backup battery": "Bateria Respaldo", "batería respaldo": "Bateria Respaldo", "bateria": "Bateria Respaldo", "pila interna": "Bateria Respaldo",
    # iButton y variantes
    "ibutton": "iButton", "identificador operador": "iButton", "llave dallas": "iButton", "lector ibutton": "iButton", "cableado de ibutton": "iButton", "llave": "iButton",
    # Chapa Electrónica
    "chapa electronica": "Chapa Electronica", "candado electronico": "Chapa Electronica", "electrochapa": "Chapa Electronica", "chapa eléctrica": "Chapa Electronica",
    # Sirena
    "sirena": "Sirena",
    # Micrófono
    "microfono": "Microfono", "escucha cabina": "Microfono", "micrófono": "Microfono", "micro": "Microfono",
    # Bocina
    "bocina": "Bocina", "altavoz": "Bocina",
    # Telemetría
    "telemetria": "Telemetria",
    # CAN Bus y variantes
    "can bus": "CAN Bus", "computadora vehiculo": "CAN Bus", "lector canbus": "CAN Bus", "can": "CAN Bus", "easy can": "CAN Bus", "easycan": "CAN Bus", "canst20": "CAN Bus", "can-st20": "CAN Bus",
    # Cámara y variantes
    "camara": "Camara", "cámara": "Camara", "camaras": "Camara", "camaras exteriores": "Camara", "camara frontal": "Camara", "camara tipo domo": "Camara", "sistema de camaras": "Camara", "camara exterior": "Camara",
    # MDVR
    "mdvr": "MDVR", "dvr": "MDVR",
    # Módulo Voz
    "modulo de voz": "Modulo Voz", "voz": "Modulo Voz", "módulo voz": "Modulo Voz",
    # Display
    "display": "Display", "pantalla": "Display",
    # Kit ADAS/DMS
    "adas": "Kit ADAS/DMS", "dms": "Kit ADAS/DMS", "kit adas": "Kit ADAS/DMS", "sistema adas": "Kit ADAS/DMS", "sistema adas y dms": "Kit ADAS/DMS", "kit adas + dms": "Kit ADAS/DMS",
    # Otros accesorios específicos del log
    "relevador": "Relevador",
    "teclado": "Teclado"
}
ACCIONES_ESTANDAR = ["Instalacion", "Desinstalacion", "Reemplazo", "Revision/Neutra", "Medicion Tanque"]

# --- Functions ---

def update_log_display(new_entry, level="INFO"):
    timestamp = datetime.datetime.now().strftime('%H:%M:%S')
    st.session_state.log_string += f"{timestamp} [{level}] {new_entry}\n"

def get_gemini_client(api_key):
    update_log_display("Attempting to configure Gemini client.", level="DEBUG")
    if not api_key:
        st.error("API Key no proporcionada.")
        update_log_display("API Key not provided for Gemini client.", level="ERROR")
        return None
    try:
        genai.configure(api_key=api_key)
        models = genai.list_models() 
        if not any('generateContent' in m.supported_generation_methods for m in models):
             st.error("La API Key es válida pero no tiene permisos para 'generateContent' o no hay modelos disponibles.")
             update_log_display("API Key valid, but no models support 'generateContent' or no models available.", level="ERROR")
             return None
        update_log_display("Gemini client configured successfully.", level="INFO")
        return genai
    except Exception as e:
        st.error(f"Error al configurar la API de Gemini. Verifica tu clave API: {e}")
        update_log_display(f"Error configuring Gemini API: {e}. Traceback: {traceback.format_exc()}", level="ERROR")
        return None

def build_gemini_prompt(descriptions_list):
    descriptions_list_string = "\n".join([f"- \"{desc}\"" for desc in descriptions_list])
    prompt = f"""
Eres un asistente experto en análisis de registros de servicio de flotas vehiculares. Dada la siguiente lista de {len(descriptions_list)} descripciones de servicio, analiza CADA descripción INDIVIDUALMENTE e identifica los componentes mencionados, la acción realizada sobre ellos, y CUALQUIER IDENTIFICADOR ÚNICO (IMEI, número de serie, MAC address como C2313007631, TDBLE_XXXX/XX:XX:XX:XX:XX:XX, o F7C74F3F64D2, Power Hub #868, PowerLite 111) asociado DIRECTAMENTE con ese componente específico en la descripción.

Componentes a buscar y estandarizar a estos nombres: {', '.join(COMPONENTES_ESTANDAR)}.
Usa el siguiente mapeo para estandarizar variantes:
{json.dumps(MAPEO_COMPONENTES, indent=2)}
Si un componente no está en la lista o no es relevante (ej. 'tornillo', 'limpieza general', 'cable', 'tierra', 'corriente', 'tarjeta sd', 'memoria', 'sim', 'fusible', 'portafusible', 'sikaflex', 'pija'), ignóralo. Ignora también nombres de marcas (Teltonika, Suntech, Queclink, GTRACK, Ruptela, Concox, Topflytech, Sinotrack, Queclink) a menos que claramente se refieran al componente GPS principal. Si la marca incluye un modelo (ej. "Teltonika FMB920"), el modelo (FMB920) puede ser parte del accesorio_id si es un GPS.
Un "relevador" solo es relevante si se menciona en un contexto de instalación/desinstalación/cambio explícito DEL RELEVADOR. No lo infieras para "paro de motor".

Acciones y sus palabras clave asociadas:
- Instalacion: 'instalacion', 'instala', 'instalar', 'inst', 'agrega', 'colocacion', 'activacion', 'conectar', 'nuevo', 'puesta en marcha', 'se instalo', 'se puso', 'instalación nueva', 'se le instala', 'se asigna', 'se le aplica', 'con instalacion de', 'se coloca'
- Desinstalacion: 'desinstalacion', 'desinstala', 'desinstalar', 'retiro', 'quita', 'baja', 'eliminar', 'desconectar', 'se retiro', 'se quito', 'retiro de', 'desisntalacion', 'equipo perdido', 'se da de baja', 'se retira', 'no regresa', 'baja en plataforma', 'desistalacion', 'desinstalación'
- Reemplazo: 'cambio', 'cambiar', 'reemplazo', 'reemplazar', 'sustitucion', 'sustituir', 'se hace cambio de', 'se cambia', 'cambiio'. (Implica que el componente SIGUE presente).
- Revision/Neutra: 'revision', 'revisar', 'mantenimiento', 'diagnostico', 'chequeo', 'verificacion', 'configuracion', 'falla', 'problema', 'ajuste', 'soporte', 'prueba', 'limpieza', 'actualizacion', 'no funciona', 'reporta', 'visita tecnica', 'reset', 'se hizo un reset', 'se checa', 'se verifica', 'se conecta', 'se reconecta', 'energizada', 'reubicó', 'desconecta arnes', 'se aplica reset', 'se cambia conexion', 'se cambia tierra', 'se cambia corriente', 'reacomodan', 'calibracion', 'cotejo', 'se fija', 'se ajusta', 'revisan conexiones', 'se energiza', 'se restablece', 'se monitorea', 'se reubica', 'se corrige', 'se repara', 'se activa', 'se asigna este equipo', 'se recupera equipo'. (NO cambia el estado de instalación).
- Medicion Tanque: 'medicion de tanque', 'medir tanque', 'calibracion tanque', 'aforar', 'aforo', 'verificacion de nivel', 'medicion inicial', 'registro de nivel', 'chequeo de nivel', 'se midio el tanque', 'medicion diesel', 'medicion gasolina', 'se tomaron niveles', 'medición de nivel'. (NO cambia el estado de instalación, es similar a 'Revision/Neutra' pero específica para niveles de fluidos, usualmente asociada con 'Sensor Combustible').

Interpretaciones especiales:
- "SE QUITO [ID_NUMERICO_LARGO] Teltonika FMB920": Esto es `Desinstalacion` del `GPS`, y el [ID_NUMERICO_LARGO] es el `accesorio_id` para ESE GPS.
- "SE PUSO EASY CAN C2313007631": `Instalacion` de `CAN Bus`, y `accesorio_id` es "C2313007631".
- "SE INSTALO 2 SENSORES DE TEMPERATURA CABLEADOS": `Instalacion` de `Sensor Temperatura`.
- "2 cambios de barras de combustible /C6BF2AEEEE4A /C2823E7A4184": `Reemplazo` de `Sensor Combustible`. `accesorio_id` debería ser "C6BF2AEEEE4A, C2823E7A4184".
- "SE PUSO POWER HUB #868": `Instalacion` de `Power Hub`, `accesorio_id` es "868".
- "Se realizó aforo de tanque para sensor de combustible": `Medicion Tanque` para `Sensor Combustible`. Si el ID del sensor está, inclúyelo.
- "Medición de tanque": Si no se menciona explícitamente un "Sensor Combustible" pero el contexto es claro, asocia la acción "Medicion Tanque" al componente "Sensor Combustible".
- Si un componente tiene múltiples IDs, lista los IDs en `accesorio_id` como una cadena separada por comas. Si no hay ID específico, `accesorio_id` debe ser nulo.
- Si la descripción es solo un ID (ej. "C2313007597"), asume `Instalacion` de `CAN Bus` con ese ID.
- "SOLO RASTREO": `Revision/Neutra` del `GPS`. "reinstalacion de equipo solo rastreo": `Instalacion` de `GPS`.
- "SE HIZO UN RESET": `Revision/Neutra` del `GPS`.
- "se le retira corte de motor": `Desinstalacion` de `Paro de Motor`.
- "se retira equipo": `Desinstalacion` de `GPS`.
- "Texto completamente irrelevante o confuso": DEBE resultar en `{{ "eventos_detectados": [] }}`.

Para CADA una de las {len(descriptions_list)} descripciones de entrada, devuelve un objeto JSON con la clave "eventos_detectados", que es una lista de objetos. Cada objeto debe tener "componente", "accion", y opcionalmente "accesorio_id".

**REGLA CRÍTICA E INQUEBRANTABLE:** La respuesta DEBE SER una lista JSON que contenga EXACTAMENTE {len(descriptions_list)} elementos.
Cada elemento de la lista JSON DEBE corresponder a una descripción de entrada, en el MISMO ORDEN.
* Si por CUALQUIER MOTIVO (incluyendo incapacidad de análisis, error interno del modelo, o falta de componentes/acciones relevantes en una descripción) no puedes procesar una descripción específica o no encuentras nada relevante, DEBES OBLIGATORIAMENTE incluir `{{ "eventos_detectados": [] }}` en la posición correspondiente a esa descripción en la lista de salida.
* NO OMITAS NINGÚN ELEMENTO DE LA LISTA. La longitud de la lista de salida DEBE SER SIEMPRE {len(descriptions_list)}. NO PUEDE SER MENOR.

No incluyas explicaciones adicionales. Solo la lista JSON pura y válida con exactamente {len(descriptions_list)} elementos.

Ejemplos de Entrada (Lista de 8 descripciones):
- "SE Retiro de paro de motor"
- "INST EASY CAN C2313007631 TDBLE_308529/DD:2B:C1:75:2F:FA TDBLE_308552/EE:9B:27:5B:78:38 TDBLE_308545/E0:AE:76:02:35:83"
- "SE QUITO 359632107908086 Teltonika FMB920"
- "SE PUSO POWER HUB 868"
- "2 cambios de barras de combustible /C6BF2AEEEE4A /C2823E7A4184"
- "SE HIZO UN RESET"
- "Medición de tanque para unidad con sensor de combustible TDBLE_123456"
- "Esta es una descripción sin componentes relevantes."

Ejemplo de Salida Esperada (Lista JSON con 8 elementos):
[
  {{ "eventos_detectados": [{{ "componente": "Paro de Motor", "accion": "Desinstalacion" }}] }},
  {{ "eventos_detectados": [
      {{ "componente": "CAN Bus", "accion": "Instalacion", "accesorio_id": "C2313007631" }},
      {{ "componente": "Sensor Combustible", "accion": "Instalacion", "accesorio_id": "TDBLE_308529/DD:2B:C1:75:2F:FA, TDBLE_308552/EE:9B:27:5B:78:38, TDBLE_308545/E0:AE:76:02:35:83" }}
  ]}},
  {{ "eventos_detectados": [{{ "componente": "GPS", "accion": "Desinstalacion", "accesorio_id": "359632107908086" }}] }},
  {{ "eventos_detectados": [{{ "componente": "Power Hub", "accion": "Instalacion", "accesorio_id": "868" }}] }},
  {{ "eventos_detectados": [{{ "componente": "Sensor Combustible", "accion": "Reemplazo", "accesorio_id": "C6BF2AEEEE4A, C2823E7A4184" }}] }},
  {{ "eventos_detectados": [{{ "componente": "GPS", "accion": "Revision/Neutra" }}] }},
  {{ "eventos_detectados": [{{ "componente": "Sensor Combustible", "accion": "Medicion Tanque", "accesorio_id": "TDBLE_123456" }}] }},
  {{ "eventos_detectados": [] }}
]

Ahora procesa la siguiente lista de {len(descriptions_list)} descripciones:
{descriptions_list_string}

Devuelve únicamente la lista JSON con EXACTAMENTE {len(descriptions_list)} elementos.
"""
    return prompt

def normalize_component_name(name):
    if not isinstance(name, str): return "Desconocido"
    name_lower = ' '.join(name.lower().strip().split()) 
    if name_lower in MAPEO_COMPONENTES: return MAPEO_COMPONENTES[name_lower]
    
    sorted_keys = sorted(MAPEO_COMPONENTES.keys(), key=len, reverse=True)
    for key in sorted_keys:
        if key and key in name_lower: 
            if re.search(r'\b' + re.escape(key) + r'\b', name_lower, re.IGNORECASE) or \
               name_lower.startswith(key + ' ') or \
               name_lower.endswith(' ' + key) or \
               (' ' + key + ' ') in name_lower or \
               name_lower == key : 
                 return MAPEO_COMPONENTES[key]
                 
    name_title_case_norm = name.strip().title()
    if name_title_case_norm in COMPONENTES_ESTANDAR: return name_title_case_norm

    name_upper_norm = name.strip().upper()
    if name_upper_norm in COMPONENTES_ESTANDAR: return name_upper_norm

    name_original_norm = ' '.join(name.strip().split())
    if name_original_norm in COMPONENTES_ESTANDAR: return name_original_norm
    
    return "Desconocido"

total_batches_global = 0

def extract_events_with_gemini(genai_client, descriptions_batch, batch_index, retries=2, delay=5):
    global total_batches_global
    update_log_display(f"Entering extract_events_with_gemini for batch {batch_index + 1}", level="DEBUG")

    if not genai_client:
        error_msg = f"[Lote {batch_index + 1}] CRITICAL: Cliente Gemini no inicializado."
        update_log_display(error_msg, level="CRITICAL")
        return [{"eventos_detectados": []} for _ in range(len(descriptions_batch))]

    model_name = "gemini-2.5-flash-preview-04-17" 
    try:
        model = genai_client.GenerativeModel(model_name)
        update_log_display(f"[Lote {batch_index + 1}] Gemini model '{model_name}' initialized.", level="DEBUG")
    except Exception as model_error:
        error_msg = f"[Lote {batch_index + 1}] CRITICAL: Error al inicializar modelo Gemini '{model_name}': {model_error}. Traceback: {traceback.format_exc()}"
        update_log_display(error_msg, level="CRITICAL")
        return [{"eventos_detectados": []} for _ in range(len(descriptions_batch))]

    prompt = build_gemini_prompt(descriptions_batch)
    update_log_display(f"\n===== Lote {batch_index + 1}/{total_batches_global} (Tamaño: {len(descriptions_batch)}) =====", level="INFO")

    attempt = 0
    last_error = None
    last_error_details = ""
    validated_results = None

    while attempt <= retries:
        update_log_display(f"[Lote {batch_index + 1} Intento {attempt + 1}/{retries + 1}] Llamando a la API {model_name}...", level="INFO")
        raw_response_text = ""
        response_obj = None

        try:
            spinner_msg = f"Lote {batch_index + 1}/{total_batches_global}: Llamando a Gemini (Intento {attempt + 1}/{retries + 1})..."
            with st.spinner(spinner_msg):
                if attempt > 0:
                    sleep_time = delay * (2 ** attempt)
                    update_log_display(f"[Lote {batch_index + 1} Intento {attempt + 1}] Esperando {sleep_time}s antes de reintentar...", level="INFO")
                    time.sleep(sleep_time)

                api_call_start_time = time.time()
                response_obj = model.generate_content(
                    prompt,
                    generation_config=genai.types.GenerationConfig(
                        temperature=0.05, 
                        response_mime_type="application/json",
                    ),
                    request_options={'timeout': 300}
                 )
                api_call_end_time = time.time()
                update_log_display(f"[Lote {batch_index + 1} Intento {attempt + 1}] Llamada a API completada en {api_call_end_time - api_call_start_time:.2f}s.", level="INFO")

            if response_obj:
                if hasattr(response_obj, 'prompt_feedback') and response_obj.prompt_feedback:
                    block_reason = getattr(response_obj.prompt_feedback, 'block_reason', "N/A")
                    update_log_display(f"[Lote {batch_index + 1} Intento {attempt + 1}] Prompt Feedback - Block Reason: {block_reason}", level="DEBUG")
                    for rating in getattr(response_obj.prompt_feedback, 'safety_ratings', []):
                        update_log_display(f"[Lote {batch_index + 1} Intento {attempt + 1}] Safety Rating: Category '{rating.category}', Probability '{rating.probability}'", level="DEBUG")
                
                if hasattr(response_obj, 'candidates') and response_obj.candidates:
                    candidate = response_obj.candidates[0]
                    finish_reason_value = getattr(candidate, 'finish_reason', "N/A")
                    # Convert enum to string if possible for logging
                    finish_reason_str = str(finish_reason_value.name if hasattr(finish_reason_value, 'name') else finish_reason_value)
                    update_log_display(f"[Lote {batch_index + 1} Intento {attempt + 1}] Candidate Finish Reason: {finish_reason_str}", level="DEBUG")


                try:
                    if hasattr(response_obj, 'parts') and response_obj.parts:
                        raw_response_text = response_obj.text.strip()
                    elif hasattr(response_obj, 'candidates') and response_obj.candidates and getattr(response_obj.candidates[0], 'finish_reason', '') != genai.types.Candidate.FinishReason.STOP: # Compare with actual enum value
                        finish_reason_candidate = getattr(response_obj.candidates[0], 'finish_reason', "UNKNOWN_REASON")
                        update_log_display(f"[Lote {batch_index + 1} Intento {attempt + 1}] Respuesta con finish_reason problemático: '{finish_reason_candidate}'. Podría estar bloqueada o incompleta.", level="WARNING")
                        raw_response_text = ""
                    else:
                        update_log_display(f"[Lote {batch_index + 1} Intento {attempt + 1}] Respuesta sin 'parts' o 'parts' vacías. Podría estar vacía o bloqueada.", level="WARNING")
                        raw_response_text = ""
                except ValueError as ve:
                     update_log_display(f"[Lote {batch_index + 1} Intento {attempt + 1}] Error al obtener texto de la respuesta (puede indicar bloqueo por contenido o filtro de seguridad): {ve}", level="WARNING")
                     raw_response_text = ""
                except AttributeError:
                     update_log_display(f"[Lote {batch_index + 1} Intento {attempt + 1}] Error: Estructura de respuesta inesperada (AttributeError).", level="ERROR")
                     raw_response_text = ""
                except Exception as e_resp_text:
                     update_log_display(f"[Lote {batch_index + 1} Intento {attempt + 1}] Error inesperado al obtener texto de la respuesta: {e_resp_text}", level="ERROR")
                     raw_response_text = ""

                update_log_display(f"[Lote {batch_index + 1} Intento {attempt + 1}] Respuesta Cruda Recibida (primeros 500 chars):\n---\n{raw_response_text[:500]}...\n---", level="DEBUG")
            else: 
                update_log_display(f"[Lote {batch_index + 1} Intento {attempt + 1}] Objeto de respuesta de API NULO o vacío.", level="ERROR")
                last_error = ValueError("Respuesta de API nula o vacía.")
                last_error_details = "No response object received from API."
                attempt += 1
                continue 

            is_blocked_response = False
            if not raw_response_text: 
                if response_obj and hasattr(response_obj, 'prompt_feedback') and getattr(response_obj.prompt_feedback, 'block_reason', None):
                    is_blocked_response = True
                    block_reason_detail = getattr(response_obj.prompt_feedback, 'block_reason', "UNKNOWN")
                    update_log_display(f"[Lote {batch_index + 1} Intento {attempt + 1}] Respuesta vacía. Prompt Feedback indica bloqueo: {block_reason_detail}. No se reintentará.", level="ERROR")
                elif response_obj and hasattr(response_obj, 'candidates') and response_obj.candidates:
                    candidate_finish_reason = getattr(response_obj.candidates[0], 'finish_reason', genai.types.Candidate.FinishReason.UNSPECIFIED)
                    # Check against problematic finish reasons
                    problematic_reasons = [
                        genai.types.Candidate.FinishReason.SAFETY,
                        genai.types.Candidate.FinishReason.RECITATION,
                        genai.types.Candidate.FinishReason.OTHER
                    ]
                    if candidate_finish_reason in problematic_reasons:
                        is_blocked_response = True
                        update_log_display(f"[Lote {batch_index + 1} Intento {attempt + 1}] Respuesta vacía. Candidate Finish Reason indica problema: {candidate_finish_reason.name}. No se reintentará.", level="ERROR")
                
                if is_blocked_response:
                    last_error = ValueError("Respuesta de API vacía debido a bloqueo (seguridad/contenido/otro).")
                    last_error_details = "API response was empty, likely due to safety filters, content policy, or other model-side issue."
                    validated_results = None 
                    break 
                else: 
                    update_log_display(f"[Lote {batch_index + 1} Intento {attempt + 1}] Respuesta de API vacía (texto). Reintentando...", level="WARNING")
                    last_error = ValueError("Respuesta de API vacía (texto).")
                    last_error_details = "No text content in response."
                    attempt += 1
                    continue

            cleaned_response_text = raw_response_text
            match = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', cleaned_response_text, re.IGNORECASE)
            if match: cleaned_response_text = match.group(1).strip()
            
            first_bracket = cleaned_response_text.find('[')
            last_bracket = cleaned_response_text.rfind(']')
            if first_bracket != -1 and last_bracket != -1 and last_bracket > first_bracket:
                 potential_json = cleaned_response_text[first_bracket : last_bracket + 1]
                 if potential_json.startswith('[') and potential_json.endswith(']') and \
                    potential_json.count('[') == potential_json.count(']') and \
                    potential_json.count('{') == potential_json.count('}'):
                      cleaned_response_text = potential_json
            
            update_log_display(f"[Lote {batch_index + 1} Intento {attempt + 1}] Respuesta Limpiada (intentada para parseo):\n---\n{cleaned_response_text[:500]}...\n---", level="DEBUG")

            current_results = None
            try:
                if not cleaned_response_text: raise json.JSONDecodeError("Cadena vacía para parsear JSON", "", 0)
                current_results = json.loads(cleaned_response_text)
                update_log_display(f"[Lote {batch_index + 1} Intento {attempt + 1}] Parseo JSON exitoso.", level="INFO")
            except json.JSONDecodeError as json_e:
                last_error = json_e
                context_around_error = cleaned_response_text[max(0, json_e.pos-20):min(len(cleaned_response_text), json_e.pos+20)]
                last_error_details = f"Pos: {json_e.pos}, Line: {json_e.lineno}, Col: {json_e.colno}. Contexto: '...{context_around_error}...'. Texto (500c): {cleaned_response_text[:500]}..."
                update_log_display(f"[Lote {batch_index + 1} Intento {attempt + 1}] Error Parseo JSON: {json_e}. Det: {last_error_details}", level="ERROR")
                attempt += 1
                continue

            if not isinstance(current_results, list):
                last_error = TypeError(f"Respuesta JSON no es lista. Tipo: {type(current_results)}")
                last_error_details = str(current_results)[:500]
                update_log_display(f"[Lote {batch_index + 1} Intento {attempt + 1}] Error: {last_error}", level="ERROR")
                attempt += 1
                continue

            if len(current_results) != len(descriptions_batch):
                last_error = ValueError(f"Longitud JSON incorrecta (Esperada: {len(descriptions_batch)}, Recibida: {len(current_results)}).")
                last_error_details = f"Primeros elementos: {str(current_results[:5])}" if current_results else "Lista vacía."
                update_log_display(f"[Lote {batch_index + 1} Intento {attempt + 1}] Error: {last_error}", level="WARNING")
                if attempt < retries:
                    attempt += 1
                    continue
                else: 
                    update_log_display(f"[Lote {batch_index + 1}] Longitud incorrecta en último intento. Se forzará.", level="WARNING")
                    validated_results = current_results 
                    break 

            update_log_display(f"[Lote {batch_index + 1} Intento {attempt + 1}] Longitud OK. Validando estructura y normalizando...", level="INFO")
            temp_validated_results = []
            valid_structure_overall = True
            for i, item in enumerate(current_results):
                if isinstance(item, dict) and "eventos_detectados" in item and isinstance(item["eventos_detectados"], list):
                    normalized_events = []
                    for event_idx, event in enumerate(item["eventos_detectados"]):
                        if isinstance(event, dict) and "componente" in event and "accion" in event:
                            comp = normalize_component_name(event.get("componente"))
                            acc_raw = event.get("accion")
                            accesorio_id_raw = event.get("accesorio_id")
                            accesorio_id_str = (", ".join(str(x).strip() for x in accesorio_id_raw if x is not None and str(x).strip()) 
                                                if isinstance(accesorio_id_raw, list) 
                                                else (str(accesorio_id_raw).strip() if accesorio_id_raw is not None and str(accesorio_id_raw).strip() else None))

                            acc_norm = "Revision/Neutra"
                            acc_lower = str(acc_raw).lower().strip()
                            acciones_estandar_lower_map = {a.lower(): a for a in ACCIONES_ESTANDAR}

                            if acc_lower in acciones_estandar_lower_map:
                                acc_norm = acciones_estandar_lower_map[acc_lower]
                            else: 
                                if any(kw in acc_lower for kw in ['instalacion', 'instala', 'instalar', 'inst', 'agrega', 'colocacion', 'activacion', 'conectar', 'nuevo', 'puesta en marcha', 'se instalo', 'se puso', 'instalación nueva', 'se le instala', 'se asigna', 'se le aplica', 'con instalacion de', 'se coloca']): acc_norm = "Instalacion"
                                elif any(kw in acc_lower for kw in ['desinstalacion', 'desinstala', 'desinstalar', 'retiro', 'quita', 'baja', 'eliminar', 'desconectar', 'se retiro', 'se quito', 'retiro de', 'desisntalacion', 'equipo perdido', 'se da de baja', 'se retira', 'no regresa', 'baja en plataforma', 'desistalacion', 'desinstalación']): acc_norm = "Desinstalacion"
                                elif any(kw in acc_lower for kw in ['cambio', 'cambiar', 'reemplazo', 'reemplazar', 'sustitucion', 'sustituir', 'se hace cambio de', 'se cambia', 'cambiio']): acc_norm = "Reemplazo"
                                elif any(kw in acc_lower for kw in ['medicion de tanque', 'medir tanque', 'calibracion tanque', 'aforar', 'aforo', 'verificacion de nivel', 'medicion inicial', 'registro de nivel', 'chequeo de nivel', 'se midio el tanque', 'medicion diesel', 'medicion gasolina', 'se tomaron niveles', 'medición de nivel']): acc_norm = "Medicion Tanque"
                                elif any(kw in acc_lower for kw in ['revision', 'revisar', 'mantenimiento', 'diagnostico', 'chequeo', 'verificacion', 'configuracion', 'falla', 'problema', 'ajuste', 'soporte', 'prueba', 'limpieza', 'actualizacion', 'no funciona', 'reporta', 'visita tecnica', 'reset', 'se hizo un reset', 'se checa', 'se verifica', 'se conecta', 'se reconecta', 'energizada', 'reubicó', 'desconecta arnes', 'se aplica reset', 'se cambia conexion', 'se cambia tierra', 'se cambia corriente', 'reacomodan', 'calibracion', 'cotejo', 'se fija', 'se ajusta', 'revisan conexiones', 'se energiza', 'se restablece', 'se monitorea', 'se reubica', 'se corrige', 'se repara', 'se activa', 'se asigna este equipo', 'se recupera equipo']): acc_norm = "Revision/Neutra"
                                else:
                                    update_log_display(f"[Lote {batch_index + 1} Desc {i+1} Ev {event_idx+1}] WARN: Acción '{acc_raw}' no estándar. Default: 'Revision/Neutra'.", level="WARNING")
                            
                            if comp != "Desconocido":
                                normalized_events.append({"componente": comp, "accion": acc_norm, "accesorio_id": accesorio_id_str})
                            else:
                                update_log_display(f"[Lote {batch_index + 1} Desc {i+1} Ev {event_idx+1}] INFO: Comp. '{event.get('componente')}' desconocido. Ignorando.", level="INFO")
                        else:
                            update_log_display(f"[Lote {batch_index + 1} Desc {i+1} Ev {event_idx+1}] WARN: Formato evento inválido: {str(event)[:100]}. Ignorando.", level="WARNING")
                    temp_validated_results.append({"eventos_detectados": normalized_events})
                else:
                    update_log_display(f"[Lote {batch_index + 1} Desc {i+1}] WARN: Formato resultado inválido: {str(item)[:100]}. Usando vacío.", level="WARNING")
                    temp_validated_results.append({"eventos_detectados": []})
                    valid_structure_overall = False
            
            validated_results = temp_validated_results
            msg_level = "INFO" if valid_structure_overall else "WARNING"
            update_log_display(f"[Lote {batch_index + 1} Intento {attempt + 1}] Éxito {'completo' if valid_structure_overall else 'parcial'}. Estructura y normalización OK.", level=msg_level)
            update_log_display(f"Exiting extract_events_with_gemini for batch {batch_index + 1} successfully after {attempt + 1} attempts.", level="DEBUG")
            return validated_results

        except Exception as e: 
            last_error = e
            last_error_details = traceback.format_exc()
            update_log_display(f"[Lote {batch_index + 1} Intento {attempt + 1}] Error Inesperado: {e.__class__.__name__}: {e}", level="ERROR")
            update_log_display(f"Traceback: {last_error_details}", level="DEBUG")
        
        attempt += 1
        if attempt <= retries:
            update_log_display(f"[Lote {batch_index + 1}] Fallido intento {attempt}. Reintentando...", level="INFO")

    # --- FIN DEL LOOP DE REINTENTOS ---
    if validated_results is None or len(validated_results) != len(descriptions_batch):
        final_error_msg = f"[Lote {batch_index + 1}] CRITICAL: Fallaron todos los intentos ({retries + 1}) o la longitud final es incorrecta."
        if last_error: final_error_msg += f" Último error: {last_error.__class__.__name__}: {last_error}."
        update_log_display(final_error_msg, level="CRITICAL")
        if last_error_details: update_log_display(f"Últimos detalles del error: {last_error_details}", level="DEBUG")
        
        # MODIFICADO: Mensaje de error en UI más preciso
        num_received_display = len(validated_results) if validated_results else 0
        st.warning(f"Problema con el Lote {batch_index + 1} después de {retries + 1} intentos. Se recibieron {num_received_display} de {len(descriptions_batch)} resultados. Se usarán los resultados recibidos y se rellenará el resto con placeholders vacíos. Último error: {last_error}.")
        
        forced_results = []
        num_received_for_forcing = len(validated_results) if validated_results else 0
        update_log_display(f"[Lote {batch_index + 1}] Forzando resultados. Esperado: {len(descriptions_batch)}, Recibido (antes de forzar): {num_received_for_forcing}.", level="WARNING")
        for i in range(len(descriptions_batch)):
            if validated_results and i < len(validated_results) and isinstance(validated_results[i], dict) and "eventos_detectados" in validated_results[i]:
                forced_results.append(validated_results[i]) 
            else:
                forced_results.append({"eventos_detectados": []})
        update_log_display(f"Exiting extract_events_with_gemini for batch {batch_index + 1} WITH FORCED RESULTS (parciales + placeholders).", level="WARNING")
        return forced_results
    
    update_log_display(f"Exiting extract_events_with_gemini for batch {batch_index + 1} successfully (results from loop).", level="DEBUG")
    return validated_results

# ... (El resto del código desde process_data hacia abajo no necesita cambios para este ajuste específico) ...
# ... PEGAR EL RESTO DEL CÓDIGO DESDE AQUÍ ...
def process_data(df_filtered, api_key, imei_col, desc_col, date_col, client_col, batch_size=25):
    global total_batches_global
    update_log_display(f"Entering process_data. Rows: {len(df_filtered)}, Batch size: {batch_size}", level="DEBUG")

    genai_client = get_gemini_client(api_key)
    st.session_state.processing_complete = False

    if not genai_client:
        error_msg = "CRITICAL: Cliente Gemini no inicializado. Verifique API Key. Procesamiento detenido."
        update_log_display(error_msg, level="CRITICAL"); st.error(error_msg)
        return pd.DataFrame(columns=["IMEI", "Fecha", "Cliente", "Componente", "Accion", "Accesorio_ID", "Descripcion_Original"]), error_msg

    update_log_display(f"Cols: IMEI='{imei_col}', Cliente='{client_col}', Desc='{desc_col}', Fecha='{date_col}'", level="INFO")
    update_log_display(f"Batch Size: {batch_size}", level="INFO")

    all_extracted_events = []
    total_rows = len(df_filtered)
    event_cols = ["IMEI", "Fecha", "Cliente", "Componente", "Accion", "Accesorio_ID", "Descripcion_Original"]

    if total_rows == 0:
        no_data_msg = "No hay datos válidos para procesar con los filtros actuales."
        update_log_display(no_data_msg, level="WARNING"); st.warning(no_data_msg)
        st.session_state.processing_complete = True
        return pd.DataFrame(columns=event_cols), no_data_msg

    processed_rows_count = 0
    batches_with_critical_issues = 0 
    total_batches_global = (total_rows + batch_size - 1) // batch_size
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    status_text.text(f"Iniciando {total_rows} filas en {total_batches_global} lotes...")
    update_log_display(f"Total filas: {total_rows} ({total_batches_global} lotes de ~{batch_size})", level="INFO")

    log_placeholder_key_base = "log_area_runtime_process_data" 
    log_placeholder = st.empty()
    log_placeholder.text_area("Log de Procesamiento (Batch):", value=st.session_state.log_string, height=300, disabled=True, key=f"{log_placeholder_key_base}_initial")

    start_process_time = time.time()

    for i in range(0, total_rows, batch_size):
        batch_start_time = time.time()
        current_batch_index = i // batch_size
        batch_number = current_batch_index + 1

        log_placeholder.text_area("Log de Procesamiento (Batch):", value=st.session_state.log_string, height=300, disabled=True, key=f"{log_placeholder_key_base}_{batch_number}")

        batch_df = df_filtered.iloc[i:min(i + batch_size, total_rows)]
        descriptions_batch = batch_df[desc_col].fillna('').astype(str).tolist()

        if not descriptions_batch:
             update_log_display(f"[Lote {batch_number}/{total_batches_global}] Omitiendo lote vacío.", level="WARNING")
             continue

        update_log_display(f"\n[Lote {batch_number}/{total_batches_global}] Procesando {len(descriptions_batch)} desc. (Índices DF: {batch_df.index[0]}-{batch_df.index[-1]}).", level="INFO")
        
        batch_results = extract_events_with_gemini(genai_client, descriptions_batch, current_batch_index)

        if batch_results is None or len(batch_results) != len(batch_df):
            update_log_display(f"[Lote {batch_number}] CRITICAL ERROR: batch_results longitud {len(batch_results) if batch_results else 'None'} != esperada {len(batch_df)}. Omitiendo.", level="CRITICAL")
            batches_with_critical_issues += 1
        else:
            update_log_display(f"[Lote {batch_number}] Mapeando resultados...", level="DEBUG")
            is_batch_fully_dummied = all(not res.get("eventos_detectados") for res in batch_results)
            if len(batch_df) > 0 and is_batch_fully_dummied:
                 update_log_display(f"[Lote {batch_number}] INFO: Lote completo ({len(batch_df)} desc.) resultó en eventos vacíos (posible fallo API/bloqueo).", level="INFO")
                 batches_with_critical_issues +=1

            for original_df_idx, (_, row) in enumerate(batch_df.iterrows()):
                result_for_row = batch_results[original_df_idx]
                
                if result_for_row and "eventos_detectados" in result_for_row:
                    if not result_for_row["eventos_detectados"]:
                         update_log_display(f"[Lote {batch_number} Fila {original_df_idx+1} (DF Idx {row.name})] No eventos. Desc: \"{str(row[desc_col])[:30]}...\"", level="DEBUG")
                    for event_idx, event in enumerate(result_for_row["eventos_detectados"]):
                        all_extracted_events.append({
                            "IMEI": row[imei_col], "Fecha": row[date_col], "Cliente": row[client_col],
                            "Componente": event["componente"], "Accion": event["accion"],
                            "Accesorio_ID": event.get("accesorio_id"), "Descripcion_Original": row[desc_col]
                        })
                else:
                    update_log_display(f"[Lote {batch_number} Fila {original_df_idx+1} (DF Idx {row.name})] WARN: Falta 'eventos_detectados'. Desc: \"{str(row[desc_col])[:30]}...\"", level="WARNING")
            update_log_display(f"[Lote {batch_number}] Mapeo completado. {len(batch_df)} filas procesadas.", level="INFO")

        processed_rows_count += len(batch_df)
        progress = min(1.0, processed_rows_count / total_rows) if total_rows > 0 else 0.0
        progress_bar.progress(progress)
        
        batch_end_time = time.time()
        elapsed_batch = batch_end_time - batch_start_time
        total_elapsed = batch_end_time - start_process_time
        avg_time_per_row = total_elapsed / processed_rows_count if processed_rows_count > 0 else 0
        remaining_time = max(0, (avg_time_per_row * total_rows) - total_elapsed) if avg_time_per_row > 0 else (total_batches_global - batch_number) * batch_size * 1 
        
        status_text.text(f"Procesando: {processed_rows_count}/{total_rows}. Lote {batch_number}/{total_batches_global} ({elapsed_batch:.1f}s). Rest: ~{remaining_time:.0f}s")
        update_log_display(f"Stats Lote {batch_number}: T Lote: {elapsed_batch:.2f}s. T Total: {total_elapsed:.2f}s. T Prom/Fila: {avg_time_per_row:.3f}s", level="DEBUG")

        if batch_number < total_batches_global and batch_size > 10 : time.sleep(0.2) 

    end_process_time = time.time()
    total_duration = end_process_time - start_process_time
    update_log_display(f"\n--- Fin del Procesamiento IA ({datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ---", level="INFO")
    update_log_display(f"Duración total IA: {total_duration:.2f} segundos.", level="INFO")

    completion_message = f"Procesamiento IA completado. {len(all_extracted_events)} eventos extraídos de {total_rows} filas ({processed_rows_count} procesadas)."
    if batches_with_critical_issues > 0:
        completion_message += f" {batches_with_critical_issues}/{total_batches_global} lote(s) tuvieron problemas críticos y/o resultaron en datos vacíos forzados."
        st.warning(f"{batches_with_critical_issues}/{total_batches_global} lote(s) con problemas. Resultados podrían ser placeholders. Revise log.")
    elif total_rows > 0:
        st.success("Todos los lotes procesados por IA.")

    status_text.text(completion_message)
    update_log_display(completion_message, level="INFO")
    st.session_state.processing_complete = True

    log_placeholder.text_area("Log de Procesamiento (Batch):", value=st.session_state.log_string, height=300, disabled=True, key=f"{log_placeholder_key_base}_final")

    if not all_extracted_events:
        final_msg = completion_message
        if total_rows > 0 and batches_with_critical_issues == total_batches_global: final_msg += " Todos los lotes fallaron críticamente."
        elif total_rows > 0: final_msg += " No se extrajeron eventos válidos."
        update_log_display(final_msg, level="WARNING")
        return pd.DataFrame(columns=event_cols), final_msg

    events_df = pd.DataFrame(all_extracted_events, columns=event_cols)
    try: events_df['Fecha'] = pd.to_datetime(events_df['Fecha'])
    except Exception as e: update_log_display(f"Error convirtiendo 'Fecha' final a datetime: {e}. Data: {events_df['Fecha'].head()}", level="ERROR")

    if 'Cliente' in events_df.columns: events_df['Cliente'] = events_df['Cliente'].fillna('').astype(str)
    if 'Accesorio_ID' in events_df.columns: events_df['Accesorio_ID'] = events_df['Accesorio_ID'].fillna('').astype(str)
    
    update_log_display(f"Exiting process_data. Extracted {len(events_df)} events.", level="DEBUG")
    return events_df, completion_message

def calculate_current_state(events_df):
    update_log_display("Entering calculate_current_state.", level="DEBUG")
    state_cols = ["Cliente", "IMEI", "Componentes_Instalados_Fin_Periodo", "Ultima_Fecha_Evento"]
    client_col_standard = "Cliente"

    if events_df is None or events_df.empty:
        update_log_display("events_df vacío/None en calculate_current_state. Retornando vacío.", level="WARNING")
        return pd.DataFrame(columns=state_cols)

    required_cols = ["IMEI", "Fecha", client_col_standard, "Componente", "Accion"]
    missing = [col for col in required_cols if col not in events_df.columns]
    if missing:
         st.error(f"Faltan cols en events_df para estado: {', '.join(missing)}")
         update_log_display(f"Faltan cols en events_df: {', '.join(missing)}. Presentes: {events_df.columns.tolist()}", level="ERROR")
         return pd.DataFrame(columns=state_cols)

    if not pd.api.types.is_datetime64_any_dtype(events_df['Fecha']):
        update_log_display("'Fecha' no es datetime. Convirtiendo...", level="DEBUG")
        try: events_df['Fecha'] = pd.to_datetime(events_df['Fecha'], errors='coerce')
        except Exception as e:
             st.error(f"Error fatal convirtiendo 'Fecha' a datetime: {e}")
             update_log_display(f"CRITICAL: Error convirtiendo 'Fecha': {e}. Trace: {traceback.format_exc()}", level="CRITICAL")
             return pd.DataFrame(columns=state_cols)
    
    df_copy = events_df.copy()
    for col in ['IMEI', client_col_standard, 'Componente', 'Accion']:
        df_copy[col] = df_copy[col].astype(str).fillna('')
    
    df_sorted = df_copy.dropna(subset=['Fecha']) 
    for col in ['IMEI', client_col_standard, 'Componente', 'Accion']:
        df_sorted = df_sorted[df_sorted[col] != ''] 
    
    df_sorted = df_sorted.sort_values(by=[client_col_standard, "IMEI", "Fecha"])
    
    if df_sorted.empty:
        update_log_display("events_df_sorted vacío post-limpieza. No se puede calcular estado.", level="WARNING")
        return pd.DataFrame(columns=state_cols)

    current_state = {}
    last_event_date = {}

    for group_key, group in df_sorted.groupby([client_col_standard, "IMEI"], sort=False):
        cliente, imei = group_key
        installed_components = set()
        max_date_for_group = group['Fecha'].max()

        for _, row in group.iterrows():
            component = row["Componente"]
            action = row["Accion"]
            if action == "Instalacion": installed_components.add(component)
            elif action == "Desinstalacion": installed_components.discard(component)
            elif action == "Reemplazo": installed_components.add(component)

        current_state[group_key] = sorted(list(installed_components))
        last_event_date[group_key] = max_date_for_group
    
    state_list = [{"Cliente": k[0], "IMEI": k[1],
                   "Componentes_Instalados_Fin_Periodo": ", ".join(c) if c else "Ninguno",
                   "Ultima_Fecha_Evento": last_event_date.get(k)}
                  for k, c in current_state.items()]

    if not state_list:
        update_log_display("state_list vacía. No hay estados finales.", level="INFO")
        return pd.DataFrame(columns=state_cols)

    state_df = pd.DataFrame(state_list)
    if 'Ultima_Fecha_Evento' in state_df.columns and not state_df['Ultima_Fecha_Evento'].empty:
        try: state_df['Ultima_Fecha_Evento'] = pd.to_datetime(state_df['Ultima_Fecha_Evento'], errors='coerce').dt.strftime('%Y-%m-%d')
        except Exception as e: update_log_display(f"Error formateando Ultima_Fecha_Evento: {e}", level="WARNING")

    update_log_display(f"Exiting calculate_current_state. Generated {len(state_df)} state records.", level="DEBUG")
    return state_df

# --- User Interface ---
st.sidebar.header("🔑 Configuración API Gemini")
api_key_input = st.sidebar.text_input("Ingresa tu API Key de Google Gemini", type="password", value=st.session_state.api_key, key="api_key_input_ui")
if api_key_input != st.session_state.api_key :
    st.session_state.api_key = api_key_input
    update_log_display("API Key actualizada.", level="INFO")

st.sidebar.header("📄 Carga de Archivo CSV")
uploaded_file = st.sidebar.file_uploader("Selecciona tu archivo CSV", type="csv", key="csv_uploader_ui")

if uploaded_file is not None and (st.session_state.df_loaded is None or uploaded_file.name != st.session_state.file_name):
    update_log_display(f"Nuevo archivo '{uploaded_file.name}' detectado.", level="INFO")
    try:
        df_attempt = None; encoding_used = None
        try:
            uploaded_file.seek(0); df_attempt = pd.read_csv(uploaded_file); encoding_used = 'utf-8'
            update_log_display(f"CSV leído con UTF-8.", level="DEBUG")
        except UnicodeDecodeError:
            update_log_display("Fallo UTF-8, intentando latin1...", level="WARNING")
            uploaded_file.seek(0); df_attempt = pd.read_csv(uploaded_file, encoding='latin1'); encoding_used = 'latin1'
            update_log_display(f"CSV leído con latin1.", level="DEBUG")
        except pd.errors.ParserError as pe:
            st.error(f"Error de parseo CSV: {pe}. Verifique formato."); update_log_display(f"Error parseo CSV '{uploaded_file.name}': {pe}", level="ERROR")
        except Exception as e:
            st.error(f"Error crítico leyendo CSV: {e}"); update_log_display(f"Error crítico leyendo CSV '{uploaded_file.name}': {e}", level="CRITICAL")
        
        if df_attempt is not None:
            st.session_state.df_loaded = df_attempt
            st.session_state.file_name = uploaded_file.name
            st.session_state.column_options = df_attempt.columns.tolist()
            st.sidebar.success(f"Archivo '{uploaded_file.name}' ({len(df_attempt)} filas) cargado (enc: '{encoding_used}').")
            update_log_display(f"Archivo '{uploaded_file.name}' ({len(df_attempt)} filas) cargado. Enc: {encoding_used}.", level="INFO")
            for k in ['min_date', 'max_date', 'start_date', 'end_date', 'events_df', 'current_state_df']: st.session_state[k] = None
            st.session_state.selected_clients_list = ["-- TODOS --"]
            st.rerun()
    except Exception as e_load:
        st.sidebar.error(f"Error procesando archivo: {e_load}"); update_log_display(f"Error general cargando '{uploaded_file.name if uploaded_file else 'N/A'}': {e_load}", level="ERROR")
        for k in ['df_loaded', 'file_name', 'column_options', 'min_date', 'max_date']: st.session_state[k] = None if k != 'column_options' else []
        st.rerun()

df_loaded = st.session_state.df_loaded
column_options = st.session_state.column_options
min_date = st.session_state.min_date
max_date = st.session_state.max_date

st.sidebar.header("📊 Configuración de Columnas")
current_selections_for_find = {
    'imei': st.session_state.get('imei_col', None),
    'desc': st.session_state.get('desc_col', None),
    'date': st.session_state.get('date_col', None),
    'client': st.session_state.get('client_col', None)
}

if column_options:
    def find_col_default(options, keywords, current_selections_dict, field_key_being_set, default_idx_offset=0):
        available_options = [opt for opt_idx, opt in enumerate(options) 
                             if opt not in [val for key, val in current_selections_dict.items() if key != field_key_being_set and val is not None]]
        
        for kw in keywords:
            match = next((col for col in available_options if kw in col.upper()), None) 
            if match: return match
            match_overall = next((col for col in options if kw in col.upper()), None) 
            if match_overall: return match_overall

        if available_options: return available_options[min(default_idx_offset, len(available_options)-1)]
        if options: return options[min(default_idx_offset, len(options)-1)]
        return None

    imei_col_default = find_col_default(column_options, ["IMEI", "IMEI REAL"], current_selections_for_find, 'imei', 0)
    desc_col_default = find_col_default(column_options, ["DESC", "OBSERVACION", "DESCRIPTION"], current_selections_for_find, 'desc', 1)
    date_col_default = find_col_default(column_options, ["FECHA", "DATE", "TIMESTAMP"], current_selections_for_find, 'date', 2)
    client_col_default = find_col_default(column_options, ["CLIENT", "CLIENTE", "CLIENTES SATECH"], current_selections_for_find, 'client', 3)

    def get_idx(val, default_val, options_list):
        try: return options_list.index(val) if val in options_list else options_list.index(default_val) if default_val in options_list else 0
        except ValueError: return 0 

    st.session_state.imei_col = st.sidebar.selectbox("Col. IMEI:", column_options, index=get_idx(current_selections_for_find['imei'], imei_col_default, column_options), key='imei_col_select_ui')
    st.session_state.desc_col = st.sidebar.selectbox("Col. Descripción:", column_options, index=get_idx(current_selections_for_find['desc'], desc_col_default, column_options), key='desc_col_select_ui')
    st.session_state.date_col = st.sidebar.selectbox("Col. Fecha Servicio:", column_options, index=get_idx(current_selections_for_find['date'], date_col_default, column_options), key='date_col_select_ui')
    st.session_state.client_col = st.sidebar.selectbox("Col. Cliente:", column_options, index=get_idx(current_selections_for_find['client'], client_col_default, column_options), key='client_col_select_ui')
else:
    for label, key_suffix in [("IMEI", "imei"), ("Descripción", "desc"), ("Fecha Servicio", "date"), ("Cliente", "client")]:
        st.sidebar.selectbox(f"Col. {label}:", ["N/A"], disabled=True, key=f"{key_suffix}_col_select_ui_disabled")

imei_col = st.session_state.imei_col
desc_col = st.session_state.desc_col
date_col = st.session_state.date_col
client_col = st.session_state.client_col

if date_col and date_col != "N/A" and df_loaded is not None and date_col in df_loaded.columns and (min_date is None or max_date is None):
    update_log_display(f"Procesando col. fecha '{date_col}' para rango.", level="DEBUG")
    df_copy_date = df_loaded.copy()
    try:
        if not pd.api.types.is_datetime64_any_dtype(df_copy_date[date_col]):
            update_log_display(f"Col '{date_col}' no es datetime. Convirtiendo...", level="INFO")
            common_formats = ["%d/%m/%Y %H:%M:%S", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]
            converted_dates = None
            for fmt in common_formats:
                try:
                    converted_dates_try = pd.to_datetime(df_copy_date[date_col], format=fmt, errors='coerce')
                    if not converted_dates_try.isnull().all(): 
                        converted_dates = converted_dates_try
                        update_log_display(f"Fechas convertidas con formato: {fmt}", level="DEBUG")
                        break
                except Exception: continue 
            
            if converted_dates is None or converted_dates.isnull().all(): 
                update_log_display(f"Formatos comunes fallaron para '{date_col}'. Infiriendo...", level="DEBUG")
                converted_dates = pd.to_datetime(df_copy_date[date_col], errors='coerce', infer_datetime_format=True)

            valid_conversions = converted_dates.notna().sum()
            total_original = len(df_copy_date[date_col])
            if valid_conversions < total_original:
                st.warning(f"{total_original - valid_conversions} de {total_original} en '{date_col}' no pudieron ser convertidas a fecha.")
                update_log_display(f"WARN: {total_original - valid_conversions} en '{date_col}' no convertidas.", level="WARNING")
            
            if valid_conversions > 0:
                 df_copy_date[date_col] = converted_dates
                 st.session_state.df_loaded[date_col] = converted_dates 
                 update_log_display(f"Col '{date_col}' convertida. {valid_conversions} válidas.", level="INFO")
            else:
                 st.sidebar.error(f"No se convirtieron fechas en '{date_col}'."); update_log_display(f"ERROR: No fechas convertidas en '{date_col}'.", level="ERROR")
        
        df_valid_dates = df_copy_date.dropna(subset=[date_col])
        if not df_valid_dates.empty and pd.api.types.is_datetime64_any_dtype(df_valid_dates[date_col]):
            min_dt, max_dt = df_valid_dates[date_col].min(), df_valid_dates[date_col].max()
            st.session_state.min_date = min_dt.date() if pd.notnull(min_dt) else None
            st.session_state.max_date = max_dt.date() if pd.notnull(max_dt) else None
            min_date, max_date = st.session_state.min_date, st.session_state.max_date 

            st.sidebar.success(f"Rango fechas: {min_date} a {max_date}")
            update_log_display(f"Rango fechas en '{date_col}': {min_date} a {max_date}", level="INFO")
            if st.session_state.start_date is None and min_date: st.session_state.start_date = min_date
            if st.session_state.end_date is None and max_date: st.session_state.end_date = max_date
            st.rerun()
        else:
            st.sidebar.warning(f"No fechas válidas en '{date_col}'."); update_log_display(f"WARN: No fechas válidas en '{date_col}'.", level="WARNING")
            st.session_state.min_date = None; st.session_state.max_date = None; min_date = None; max_date = None
    except Exception as e_date_proc:
        st.sidebar.error(f"Error procesando fecha '{date_col}': {e_date_proc}")
        update_log_display(f"ERROR procesando fecha '{date_col}': {e_date_proc}. Trace: {traceback.format_exc()}", level="ERROR")
        st.session_state.min_date = None; st.session_state.max_date = None; min_date = None; max_date = None

st.sidebar.header("🗓️ Filtro por Rango de Fechas")
start_date_state = st.session_state.get('start_date', None)
end_date_state = st.session_state.get('end_date', None)
valid_date_range = False; today = datetime.date.today()

if min_date and max_date:
    if min_date > max_date: min_date, max_date = max_date, min_date 
    
    start_val = start_date_state if start_date_state and min_date <= start_date_state <= max_date else min_date
    end_val = end_date_state if end_date_state and min_date <= end_date_state <= max_date else max_date
    
    if start_val and end_val and start_val > end_val: start_val = end_val 

    st.session_state.start_date = st.sidebar.date_input("Fecha Inicio", value=start_val, min_value=min_date, max_value=max_date, key='start_date_picker_ui')
    # Make sure end_date's min_value is at least start_date
    min_for_end_date = st.session_state.start_date if st.session_state.start_date else min_date
    end_val_for_picker = end_val if end_val and end_val >= min_for_end_date else min_for_end_date

    st.session_state.end_date = st.sidebar.date_input("Fecha Fin", value=end_val_for_picker, min_value=min_for_end_date, max_value=max_date, key='end_date_picker_ui')

    if st.session_state.start_date and st.session_state.end_date and st.session_state.start_date <= st.session_state.end_date:
        valid_date_range = True
    else: st.sidebar.error("Rango de fechas inválido.")
else:
    st.sidebar.info("Cargue archivo y seleccione col. fecha válida."); 
    st.sidebar.date_input("Fecha Inicio", value=today, disabled=True, key='start_date_picker_ui_disabled')
    st.sidebar.date_input("Fecha Fin", value=today, disabled=True, key='end_date_picker_ui_disabled')

start_date = st.session_state.start_date
end_date = st.session_state.end_date

st.sidebar.header("👥 Filtro por Cliente")
selected_clients_state = st.session_state.get('selected_clients_list', ["-- TODOS --"])
selected_clients_to_filter = []

if df_loaded is not None and client_col and client_col != "N/A" and client_col in df_loaded.columns:
    try:
        client_options_raw = df_loaded[client_col].astype(str).fillna('').unique()
        client_options_list = sorted([c.strip() for c in client_options_raw if c.strip()])
        
        if client_options_list:
            options_ms = ["-- TODOS --"] + client_options_list
            current_valid_sel = [s for s in selected_clients_state if s in options_ms] or ["-- TODOS --"]
            
            selected_clients_ui = st.sidebar.multiselect("Selecciona Cliente(s):", options=options_ms, default=current_valid_sel, key='client_multiselect_ui')
            st.session_state.selected_clients_list = selected_clients_ui

            if "-- TODOS --" in selected_clients_ui or not selected_clients_ui:
                 selected_clients_to_filter = [] 
            else:
                 selected_clients_to_filter = [c for c in selected_clients_ui if c != "-- TODOS --"]
        else:
            st.sidebar.warning(f"No clientes válidos en '{client_col}'."); selected_clients_to_filter = []
            st.session_state.selected_clients_list = ["-- TODOS --"]
    except Exception as e_client_proc:
        st.sidebar.error(f"Error procesando col. cliente '{client_col}': {e_client_proc}")
        update_log_display(f"Error procesando cliente '{client_col}': {e_client_proc}", level="ERROR")
        selected_clients_to_filter = []; st.session_state.selected_clients_list = ["-- TODOS --"]
else:
    st.sidebar.info("Cargue archivo y seleccione col. cliente.")
    st.sidebar.multiselect("Selecciona Cliente(s):", ["-- TODOS --"], default=["-- TODOS --"], disabled=True, key='client_multiselect_ui_disabled')
    if not (df_loaded is not None and client_col and client_col != "N/A" and client_col in df_loaded.columns):
         st.session_state.selected_clients_list = ["-- TODOS --"]

st.sidebar.header("⚙️ Ajustes de Procesamiento")
batch_size_state = st.session_state.get('batch_size', default_values['batch_size'])
batch_size_ui = st.sidebar.slider("Tamaño del Lote (desc/llamada API):", min_value=5, max_value=100, value=batch_size_state, step=5, 
                                  help=f"Menor=más lento pero estable. Recomendado: {default_values['batch_size']}.", 
                                  disabled=df_loaded is None, key="batch_size_slider_ui")
st.session_state.batch_size = batch_size_ui

analyze_disabled = not (
    st.session_state.api_key and df_loaded is not None and
    imei_col and imei_col != "N/A" and desc_col and desc_col != "N/A" and
    date_col and date_col != "N/A" and client_col and client_col != "N/A" and
    valid_date_range and start_date is not None and end_date is not None
)

if st.sidebar.button("🚀 Analizar Historial", disabled=analyze_disabled, type="primary", key="analyze_button_ui"):
    st.session_state.processing_complete = False
    st.session_state.events_df = None; st.session_state.current_state_df = None
    st.session_state.log_string = "Iniciando análisis...\n"

    api_key_use = st.session_state.api_key
    imei_col_use, desc_col_use, date_col_use, client_col_use = imei_col, desc_col, date_col, client_col
    start_date_use, end_date_use = start_date, end_date
    batch_size_use = st.session_state.batch_size

    errors = []
    if not api_key_use: errors.append("API Key no ingresada.")
    if df_loaded is None: errors.append("Archivo CSV no cargado.")
    if not all([imei_col_use, desc_col_use, date_col_use, client_col_use]) or \
       any(c == "N/A" for c in [imei_col_use, desc_col_use, date_col_use, client_col_use]):
        errors.append("Columnas no seleccionadas o inválidas.")
    elif df_loaded is not None and not all(c in df_loaded.columns for c in [imei_col_use, desc_col_use, date_col_use, client_col_use]):
        errors.append("Una o más columnas seleccionadas no existen en el archivo.")
    if not valid_date_range or start_date_use is None or end_date_use is None: errors.append("Rango de fechas inválido.")
    
    if errors:
        for err in errors: st.error(err)
        update_log_display(f"Error crítico pre-análisis: {'; '.join(errors)}", level="CRITICAL")
        st.session_state.processing_complete = True; st.stop()

    st.info(f"Iniciando análisis con lote = {batch_size_use}...")
    ts_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    update_log_display(f"\n--- INICIO ANÁLISIS ({ts_now}) ---", level="INFO")
    update_log_display(f"Archivo: {st.session_state.get('file_name', 'N/A')}", level="INFO")
    update_log_display(f"Rango Fechas: {start_date_use} a {end_date_use}", level="INFO")
    update_log_display(f"Columnas: IMEI='{imei_col_use}', Cliente='{client_col_use}', Desc='{desc_col_use}', Fecha='{date_col_use}'", level="INFO")
    update_log_display(f"Clientes Filtro: {', '.join(selected_clients_to_filter) if selected_clients_to_filter else 'TODOS'}", level="INFO")
    update_log_display(f"Tamaño Lote: {batch_size_use}", level="INFO")

    try:
        df_proc = df_loaded.copy()

        if selected_clients_to_filter:
            update_log_display(f"Filtrando por {len(selected_clients_to_filter)} cliente(s)...", level="DEBUG")
            df_proc = df_proc[df_proc[client_col_use].astype(str).str.strip().isin(selected_clients_to_filter)]
            update_log_display(f"Filas post-cliente: {len(df_proc)}", level="INFO")
            if df_proc.empty:
                 st.warning("No datos para clientes seleccionados."); update_log_display("WARN: No datos para clientes.", level="WARNING")
                 st.session_state.processing_complete = True; st.session_state.events_df = pd.DataFrame(); st.session_state.current_state_df = pd.DataFrame()
                 st.stop()

        if not pd.api.types.is_datetime64_any_dtype(df_proc[date_col_use]):
             update_log_display(f"WARN: '{date_col_use}' no es datetime. Re-convirtiendo...", level="WARNING")
             try:
                 df_proc[date_col_use] = pd.to_datetime(df_proc[date_col_use], errors='coerce')
                 if df_proc[date_col_use].isnull().all():
                     st.error(f"Error: No se pudieron convertir fechas en '{date_col_use}'. Deteniendo."); update_log_display(f"CRIT: Fallo re-conversión '{date_col_use}'.", level="CRITICAL")
                     st.stop()
             except Exception as e_conv:
                 st.error(f"Error convirtiendo '{date_col_use}': {e_conv}. Deteniendo."); update_log_display(f"CRIT: Error conv. '{date_col_use}': {e_conv}", level="CRITICAL")
                 st.stop()
        
        start_dt = datetime.datetime.combine(start_date_use, datetime.time.min)
        end_dt = datetime.datetime.combine(end_date_use, datetime.time.max)
        update_log_display(f"Filtrando fechas: {start_dt.date()} a {end_dt.date()}.", level="DEBUG")
        
        date_series_filt = pd.to_datetime(df_proc[date_col_use], errors='coerce')
        df_proc = df_proc[(date_series_filt.notna()) & (date_series_filt >= start_dt) & (date_series_filt <= end_dt)].copy()
        update_log_display(f"Filas post-fecha: {len(df_proc)}", level="INFO")

        count_pre_na = len(df_proc)
        update_log_display("Limpiando NAs/vacíos en cols clave...", level="DEBUG")
        cols_na_check = [imei_col_use, desc_col_use, date_col_use, client_col_use]
        df_cleaned = df_proc.dropna(subset=cols_na_check)
        for col_key in [imei_col_use, desc_col_use, client_col_use]: 
            df_cleaned = df_cleaned[df_cleaned[col_key].astype(str).str.strip() != '']
        
        rows_dropped = count_pre_na - len(df_cleaned)
        if rows_dropped > 0:
            st.warning(f"Se ignoraron {rows_dropped} filas con vacíos en cols. clave post-filtros."); update_log_display(f"WARN: {rows_dropped} filas ignoradas (vacíos).", level="WARNING")
        update_log_display(f"Filas válidas finales para IA: {len(df_cleaned)}", level="INFO")

        if df_cleaned.empty:
            st.warning("No datos válidos para IA post-filtros/limpieza."); update_log_display("WARN: No datos para IA.", level="WARNING")
            st.session_state.processing_complete = True; st.session_state.events_df = pd.DataFrame(); st.session_state.current_state_df = pd.DataFrame()
        else:
            st.info(f"Iniciando IA para {len(df_cleaned)} filas..."); update_log_display(f"Iniciando IA para {len(df_cleaned)} filas...", level="INFO")
            
            events_res, proc_msg = process_data(df_cleaned, api_key_use, imei_col_use, desc_col_use, date_col_use, client_col_use, batch_size_use)
            st.session_state.events_df = events_res
            update_log_display(f"Resultado process_data: {proc_msg}", level="INFO")

            if events_res is not None and not events_res.empty:
                st.info("Calculando estado final..."); update_log_display("Calculando estado final...", level="INFO")
                current_state_res = calculate_current_state(events_res)
                st.session_state.current_state_df = current_state_res
                
                n_state = len(current_state_res)
                if n_state > 0: st.success(f"Estado final calculado. {n_state} registros."); update_log_display(f"Estado final: {n_state} registros.", level="INFO")
                else: st.info("No se determinó estado final."); update_log_display("INFO: No estado final.", level="INFO")
            else:
                 st.warning("No eventos extraídos, no se calcula estado final."); update_log_display("WARN: No eventos, no estado final.", level="WARNING")
                 st.session_state.current_state_df = pd.DataFrame()

        update_log_display(f"--- FIN ANÁLISIS ({datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ---", level="INFO")
        st.session_state.processing_complete = True
        st.rerun()

    except Exception as e_main:
        err_msg = f"Error inesperado en flujo principal: {e_main.__class__.__name__}: {e_main}"
        update_log_display(f"CRITICAL ERROR: {err_msg}. Trace: {traceback.format_exc()}", level="CRITICAL")
        st.error(err_msg); st.exception(e_main)
        st.session_state.events_df = None; st.session_state.current_state_df = None
        st.session_state.processing_complete = True

# --- Visualización de Resultados ---
events_df_disp = st.session_state.get('events_df', pd.DataFrame())
current_state_df_disp = st.session_state.get('current_state_df', pd.DataFrame())
analysis_done = st.session_state.get('processing_complete', False)

if analysis_done:
    st.markdown("---"); st.header("📊 Resultados del Análisis")
    s_date_disp = st.session_state.get('start_date', "N/A")
    e_date_disp = st.session_state.get('end_date', "N/A")
    sel_clients_fname_list = st.session_state.get('selected_clients_list', ["-- TODOS --"])
    
    client_fname = "TODOS"
    if isinstance(sel_clients_fname_list, list) and sel_clients_fname_list != ["-- TODOS --"]:
         client_fname = "_".join(map(str, sel_clients_fname_list)).replace(" ", "").replace("/", "-")[:30] 

    if events_df_disp is not None and not events_df_disp.empty:
        st.subheader(f"📋 Historial Eventos ({s_date_disp} a {e_date_disp})")
        events_df_fmt = events_df_disp.copy()
        if 'Fecha' in events_df_fmt.columns and pd.api.types.is_datetime64_any_dtype(events_df_fmt['Fecha']):
            try: events_df_fmt['Fecha'] = events_df_fmt['Fecha'].dt.strftime('%Y-%m-%d %H:%M:%S')
            except Exception as e: update_log_display(f"Error formateando fecha display: {e}", level="WARNING")
        st.dataframe(events_df_fmt, use_container_width=True, height=min(max(200, len(events_df_fmt)*35 + 38), 600))

        if current_state_df_disp is not None and not current_state_df_disp.empty:
             st.subheader(f"📈 Estado Componentes ({e_date_disp})")
             st.dataframe(current_state_df_disp, use_container_width=True, height=min(max(200, len(current_state_df_disp)*35 + 38), 600))
        elif current_state_df_disp is not None:
             st.subheader(f"📈 Estado Componentes ({e_date_disp})"); st.info("No datos de estado final.")

        dl_col1, dl_col2 = st.columns(2)
        with dl_col1:
            if current_state_df_disp is not None and not current_state_df_disp.empty:
                 try:
                      csv_state = current_state_df_disp.to_csv(index=False).encode('utf-8')
                      st.download_button(f"📥 Descargar Estado Final", csv_state, f'estado_final_{client_fname}_{s_date_disp}_a_{e_date_disp}.csv', 'text/csv', key='dl_state_csv')
                 except Exception as e: st.error(f"Error CSV estado: {e}")
        with dl_col2:
             try:
                  csv_events = events_df_disp.to_csv(index=False).encode('utf-8')
                  st.download_button(f"📥 Descargar Eventos", csv_events, f'eventos_extraidos_{client_fname}_{s_date_disp}_a_{e_date_disp}.csv', 'text/csv', key='dl_events_csv')
             except Exception as e: st.error(f"Error CSV eventos: {e}")
        
        st.subheader(f"🔍 Detalle por IMEI ({s_date_disp} a {e_date_disp})")
        imei_opts = []
        try:
             if 'IMEI' in events_df_disp.columns and not events_df_disp.empty:
                  imei_opts = sorted(events_df_disp['IMEI'].dropna().astype(str).unique().tolist())
        except Exception as e: st.warning(f"Error obteniendo IMEIs para detalle: {e}")

        if imei_opts:
             sel_imei_detail = st.selectbox("Selecciona IMEI:", options=[""] + imei_opts, key="imei_detail_sel_ui", help="IMEI para ver historial.")
             if sel_imei_detail:
                 hist_imei_df = events_df_disp[events_df_disp['IMEI'].astype(str) == sel_imei_detail].sort_values(by="Fecha").copy()
                 if 'Fecha' in hist_imei_df.columns and pd.api.types.is_datetime64_any_dtype(hist_imei_df['Fecha']):
                     try: hist_imei_df['Fecha'] = hist_imei_df['Fecha'].dt.strftime('%Y-%m-%d %H:%M:%S')
                     except Exception as e: update_log_display(f"Error formateando fecha detalle IMEI: {e}", level="WARNING")
                 
                 disp_cols = ['Fecha', 'Cliente', 'Componente', 'Accion', 'Accesorio_ID', 'Descripcion_Original']
                 existing_cols = [c for c in disp_cols if c in hist_imei_df.columns]
                 st.dataframe(hist_imei_df[existing_cols], use_container_width=True, height=min(max(150, len(hist_imei_df)*35 + 38), 400))
        elif not events_df_disp.empty: st.info("No IMEIs en eventos para detalle.")

    elif events_df_disp is not None: 
        st.warning("No se extrajo ningún evento para el rango y filtros seleccionados.")

st.markdown("---")
st.subheader("📝 Log de Procesamiento Detallado")
log_disp_content = st.session_state.get('log_string', "Log vacío.\n")
st.text_area("Log:", value=log_disp_content, height=400, disabled=True, key="log_display_main_ui")

if log_disp_content and log_disp_content.strip() and log_disp_content.strip() != default_values['log_string'].strip():
    log_s_d = st.session_state.get('start_date', "Ini")
    log_e_d = st.session_state.get('end_date', "Fin")
    log_clients_fname_list = st.session_state.get('selected_clients_list', ["-- TODOS --"])
    log_client_fn = "TODOS"
    if isinstance(log_clients_fname_list, list) and log_clients_fname_list != ["-- TODOS --"]:
         log_client_fn = "_".join(map(str, log_clients_fname_list)).replace(" ", "").replace("/", "-")[:30]
    try:
         log_b = log_disp_content.encode('utf-8')
         st.download_button("🐞 Descargar Log Completo", log_b, f"log_analisis_gps_{log_client_fn}_{log_s_d}_a_{log_e_d}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt", "text/plain", key='dl_log_main_btn')
    except Exception as e: st.error(f"Error preparando log para descarga: {e}")
elif not st.session_state.get('df_loaded', None) and not analysis_done:
    st.info("👋 ¡Bienvenido! Configura API Key, carga CSV, selecciona columnas y rango de fechas en la barra lateral para analizar.")

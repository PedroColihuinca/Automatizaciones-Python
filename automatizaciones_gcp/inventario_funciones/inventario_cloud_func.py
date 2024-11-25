import os
from google.cloud import functions_v1
from google.cloud.functions_v2 import FunctionServiceClient
import csv
from datetime import datetime

# Remplaza por la ruta a tu archivo de credenciales
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\\Users\\sashimi\\Desktop\\credenciales\\nicanor-cred.json"

# Mapea los códigos de estado a nombres en español
STATUS_MAP = {
    0: "DESCONOCIDO",
    1: "DESPLEGANDO",
    2: "EJECUTANDO",
    3: "ELIMINANDO",
    4: "FALLIDO",
    5: "DESPLEGADO"
}

# Mapa los tipos de eventos de GCS (Google Cloud Storage) a descripciones en español
EVENT_TYPE_MAP = {
    'google.storage.object.finalize': "GCS: Objeto creado",
    'google.storage.object.delete': "GCS: Objeto eliminado",
    'google.storage.object.archive': "GCS: Objeto archivado",
    'google.storage.object.metadataUpdate': "GCS: Metadatos de objeto actualizados"
}

# Claves que quieres omitir (información sensible)
SENSITIVE_KEYS = ['password', 'user', 'username', 'auth', 'passwd',
                  'cred', 'credential', 'secret', 'key', 'token',
                  'message_id', 'sheet_id', 'client_id_airflow_api',
                  'web_server_id_airflow_api', 'library_id', 'survey_id',
                  'directory_id', 'filter_id', 'web_hook', 'channel_id',
                  'db_host', 'db_port']

# Función auxiliar para filtrar datos sensibles de variables de entorno


def filter_sensitive_data(environment_variables):
    filtered_vars = {}
    for key, value in environment_variables.items():
        # Verifica si la clave contiene alguna palabra sensible
        if not any(sensitive in key.lower() for sensitive in SENSITIVE_KEYS):
            filtered_vars[key] = value
    return filtered_vars

# Función auxiliar para serializar objetos no serializables a JSON


def safe_serialize(obj):
    if isinstance(obj, dict):
        return str(obj)
    return str(obj)

# Función para convertir DatetimeWithNanoseconds a datetime legible


def convert_timestamp(timestamp):
    try:
        return datetime.fromtimestamp(timestamp.seconds + timestamp.nanos / 1e9).strftime('%Y-%m-%d %H:%M:%S')
    except AttributeError:
        return "N/A"

# Listar funciones de primera generación


def list_functions_v1(project_id, location):
    client = functions_v1.CloudFunctionsServiceClient()
    parent = f"projects/{project_id}/locations/{location}"
    request = functions_v1.ListFunctionsRequest(parent=parent)
    response = client.list_functions(request=request)

    functions_data = []
    for function in response.functions:
        env_variables = filter_sensitive_data(
            function.environment_variables) if function.environment_variables else {}
        env_variables_str = safe_serialize(env_variables)

        last_updated = convert_timestamp(function.update_time)
        creation_time = convert_timestamp(function.create_time) if hasattr(
            function, 'create_time') else "N/A"

        status = STATUS_MAP.get(function.status, "DESCONOCIDO")
        function_name = function.name.split('/')[-1]

        # Manejo de triggers
        trigger_description = "N/A"
        if hasattr(function, 'https_trigger') and function.https_trigger is not None:
            trigger_description = f"URL de Trigger HTTP: {
                function.https_trigger.url}"
        elif hasattr(function, 'event_trigger') and function.event_trigger is not None:
            event_type = function.event_trigger.event_type
            resource = function.event_trigger.resource
            event_description = EVENT_TYPE_MAP.get(event_type, event_type)
            trigger_description = f"{
                event_description} en el recurso: {resource}"
            if 'storage.googleapis.com' in resource:
                bucket = resource.split('/')[-1]
                trigger_description += f" (Bucket: {bucket})"

        functions_data.append({
            'nombre': function_name,
            'descripcion': function.description,
            'estado': status,
            'punto_entrada': function.entry_point,
            'entorno_ejecucion': function.runtime,
            'tiempo_de_espera': f"{function.timeout.seconds}s" if function.timeout else "N/A",
            'variables_de_entorno': env_variables_str,
            'max_instancias': function.max_instances if function.max_instances else "N/A",
            'descripcion_trigger': trigger_description,
            'ultima_actualizacion': last_updated,
            'fecha_de_creacion': creation_time,
            'generacion': 'Primera generacion'
        })

    return functions_data

# Listar funciones de segunda generación


def list_functions_v2(project_id, location):
    client = FunctionServiceClient()
    parent = f"projects/{project_id}/locations/{location}"
    response = client.list_functions(parent=parent)

    functions_data = []
    for function in response:
        env_variables = filter_sensitive_data(
            function.service_config.environment_variables) if function.service_config.environment_variables else {}
        env_variables_str = safe_serialize(env_variables)

        status = function.state.name if hasattr(
            function.state, 'name') else "DESCONOCIDO"
        function_name = function.name.split('/')[-1]

        # En segunda generación, el entry_point está en build_config
        entry_point = function.build_config.entry_point if function.build_config and function.build_config.entry_point else "N/A"

        # Triggers en segunda generación
        trigger_description = "N/A"
        if function.event_trigger is not None:
            event_type = function.event_trigger.event_type
            resource = function.event_trigger.trigger
            event_description = EVENT_TYPE_MAP.get(event_type, event_type)
            trigger_description = f"{
                event_description} en el recurso: {resource}"
            if 'storage.googleapis.com' in resource:
                bucket = resource.split('/')[-1]
                trigger_description += f" (Bucket: {bucket})"

        functions_data.append({
            'nombre': function_name,
            'descripcion': function.description,
            'estado': status,
            'punto_entrada': entry_point,
            'entorno_ejecucion': function.build_config.runtime if function.build_config else "N/A",
            'tiempo_de_espera': f"{function.service_config.timeout_seconds}s" if function.service_config.timeout_seconds else "N/A",
            'variables_de_entorno': env_variables_str,
            'max_instancias': function.service_config.max_instance_count if function.service_config.max_instance_count else "N/A",
            'descripcion_trigger': trigger_description,
            'ultima_actualizacion': function.update_time.strftime('%Y-%m-%d %H:%M:%S'),
            'fecha_de_creacion': function.create_time,
            'generacion': 'Segunda generacion'
        })

    return functions_data

# Guardar funciones en archivo CSV


def save_functions_to_csv(functions_data, filename='inventario_cloud_functions.csv'):
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            'nombre', 'descripcion', 'estado', 'punto_entrada', 'entorno_ejecucion', 'tiempo_de_espera',
            'variables_de_entorno', 'max_instancias', 'descripcion_trigger',
            'ultima_actualizacion', 'fecha_de_creacion', 'generacion'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()

        for function in functions_data:
            writer.writerow(function)


if __name__ == "__main__":
    project_id = "nicanor"
    location = "us-central1"

    # Obtener funciones de primera y segunda generación
    functions_v1_data = list_functions_v1(project_id, location)
    functions_v2_data = list_functions_v2(project_id, location)

    # Combinar todos los datos en una lista
    all_functions_data = functions_v1_data + functions_v2_data

    # Guardar en el archivo CSV
    save_functions_to_csv(all_functions_data)

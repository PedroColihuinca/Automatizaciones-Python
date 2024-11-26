import re
import pendulum
from google.oauth2 import service_account
from google.cloud.orchestration.airflow import service_v1
from google.cloud import storage
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# Cargar credenciales de la cuenta de servicio
credentials = service_account.Credentials.from_service_account_file(
    'C:\\Users\\sashimi\\Desktop\\credenciales\\credencial_nicanor.json')

# Crear cliente de la API de Composer
composer_client = service_v1.EnvironmentsClient(credentials=credentials)

# Crear cliente de la API de Google Cloud Storage
storage_client = storage.Client(credentials=credentials)


def get_dag_details(dag_file_content):
    # Extraer el nombre del DAG
    dag_name_match = re.search(r"airflow.DAG\(\s*'([^']+)'", dag_file_content)
    dag_name = dag_name_match.group(1) if dag_name_match else "Desconocido"

    # Extraer la descripción (tags en este caso)
    tags_match = re.search(r'tags=\[(.*?)\]', dag_file_content)
    description = tags_match.group(1) if tags_match else "Sin descripción"

    # Extraer la fecha de inicio
    start_date_match = re.search(
        r'start_date\s*=\s*datetime\((.*?)\)', dag_file_content)
    if start_date_match:
        start_date = eval(
            f"pendulum.datetime({start_date_match.group(1)})").to_date_string()
    else:
        start_date = "Desconocido"

    # Extraer el schedule
    schedule_match = re.search(
        r'schedule_interval\s*=\s*(.*?),', dag_file_content)
    schedule = schedule_match.group(1) if schedule_match else "Desconocido"

    # Extraer el estado del DAG (asumimos activo si no se especifica lo contrario)
    dag_state = "Activo"

    # Extraer las tareas y su orden
    tasks_section = dag_file_content.split("# Set task dependencies")[-1]
    tasks_order = re.findall(r'(\w+)\s*>>', tasks_section)
    tasks = " >> ".join(
        tasks_order) if tasks_order else "No se encontraron tareas"

    return dag_name, description, start_date, schedule, dag_state, tasks


def process_dag_file(blob):
    if blob.name.endswith('.py'):
        dag_file_content = blob.download_as_text()
        dag_name, description, start_date, schedule, dag_state, tasks = get_dag_details(
            dag_file_content)
        return {
            'archivo_dag': blob.name,
            'nombre_dag': dag_name,
            'descripcion': description,
            'fecha_inicio': start_date,
            'schedule': schedule,
            'estado_dag': dag_state,
            'tareas': tasks
        }
    return None


def list_dags_in_gcs(dag_gcs_prefix):
    if dag_gcs_prefix.startswith("gs://"):
        dag_gcs_prefix = dag_gcs_prefix[5:]
    bucket_name, prefix = dag_gcs_prefix.split("/", 1)

    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    dag_details = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_blob = {executor.submit(
            process_dag_file, blob): blob for blob in blobs}
        for future in as_completed(future_to_blob):
            result = future.result()
            if result:
                dag_details.append(result)

    return dag_details


def create_dag_inventory(project_id, location, output_file):
    parent = f"projects/{project_id}/locations/{location}"
    environments = composer_client.list_environments(parent=parent)

    dag_inventory = []

    for environment in environments:
        env_details = composer_client.get_environment(name=environment.name)
        dag_gcs_prefix = env_details.config.dag_gcs_prefix
        dag_files = list_dags_in_gcs(dag_gcs_prefix)

        version_airflow = env_details.config.software_config.image_version

        for dag in dag_files:
            dag_inventory.append({
                'nombre_entorno': env_details.name,
                'prefijo_gcs_dag': dag_gcs_prefix,
                'archivo_dag': dag['archivo_dag'],
                'nombre_dag': dag['nombre_dag'],
                'descripcion': dag['descripcion'],
                'fecha_inicio': dag['fecha_inicio'],
                'schedule': dag['schedule'],
                'estado_dag': dag['estado_dag'],
                'tareas': dag['tareas'],
                'ubicacion': location,
                'version_airflow': version_airflow
            })

    df = pd.DataFrame(dag_inventory)
    df.columns = ['nombre_entorno', 'prefijo_gcs_dag', 'archivo_dag', 'nombre_dag',
                  'descripcion', 'fecha_inicio', 'schedule', 'estado_dag', 'tareas',
                  'ubicacion', 'version_airflow']

    df.to_csv(output_file, index=False, sep=';')
    print(f"Inventario de DAGs guardado en {output_file}")


# Llamar a la función para generar el inventario
if __name__ == "__main__":
    create_dag_inventory('nicanor', 'us-central1',
                         'dags_inventory_detailed.csv')

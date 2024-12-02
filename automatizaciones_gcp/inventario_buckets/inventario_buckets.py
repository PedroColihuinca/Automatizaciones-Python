import os
from google.cloud import storage
import csv

# Configura las credenciales y el ID del proyecto
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'C:\\ruta\\a\\credenciales\\credencial.json'
PROYECTO_ID = "proyecto"


def listar_buckets():
    # Crear cliente de almacenamiento especificando el ID del proyecto
    client = storage.Client(project=PROYECTO_ID)

    # Obtener la lista de buckets del proyecto específico
    buckets = client.list_buckets()

    # Abrir archivo CSV para guardar el inventario
    with open("inventario_buckets.csv", mode="w", newline="", encoding="utf-8") as archivo:
        escritor_csv = csv.writer(archivo, delimiter=';')
        # Escribir encabezados en español
        escritor_csv.writerow(
            ["Nombre Bucket", "Archivo/Carpeta", "Fecha de Creación", "Tamaño (MB)"])

        # Recorrer los buckets y obtener detalles de archivos y carpetas
        for bucket in buckets:
            nombre_bucket = bucket.name

            # Listar blobs para obtener archivos y subcarpetas
            blobs = bucket.list_blobs()
            for blob in blobs:
                nombre_archivo = blob.name
                fecha_creacion = blob.time_created
                tamano_mb = blob.size / (1024 * 1024)  # Convertir tamaño a MB

                # Escribir datos en el archivo CSV
                escritor_csv.writerow(
                    [nombre_bucket, nombre_archivo, fecha_creacion, round(tamano_mb, 2)])

    print("Inventario de Buckets guardado en 'inventario_buckets.csv'.")


# Ejecutar la función
listar_buckets()

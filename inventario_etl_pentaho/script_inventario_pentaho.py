import os
import time
import socket
from pathlib import Path
import xml.etree.ElementTree as ET


def extraer_info_xml(ruta_archivo):
    """
    Extrae información relevante de un archivo XML (.kjb o .ktr).

    Args:
        ruta_archivo (str): Ruta al archivo XML.

    Returns:
        tuple: Información extraída (conexión a la base de datos, tablas modificadas).
    """
    database_list = []
    table_name_list = []
    source_uri_list = []
    file_name = "Desconocido"

    try:
        tree = ET.parse(ruta_archivo)
        root = tree.getroot()

        # Extraer la conexión a la base de datos
        database_elem = root.findall('.//database')
        database_list = [
            database.text for database in database_elem if database is not None]

        # Extraer tablas relacionadas al KJB/KTR
        table_name_elements = root.findall('.//tableName')
        table_name_list = [
            table.text for table in table_name_elements if table is not None]

        # Source Uri
        source_uri_elem = root.findall('.//sourceUri')
        source_uri_list = [
            source_uri.text for source_uri in source_uri_elem if source_uri is not None]

        file_name_elem = root.find('.//filename')
        file_name = file_name_elem.text if file_name_elem is not None else file_name

    except ET.ParseError:
        print(f"Error al analizar el archivo {ruta_archivo}")

    database_list = database_list if database_list else ["Desconocido"]
    table_name_list = table_name_list if table_name_list else ["Desconocido"]
    source_uri_list = source_uri_list if source_uri_list else ["Desconocido"]

    return table_name_list, database_list, source_uri_list, file_name


def crear_inventario_kjb(directorio_principal, archivo_salida="inventario_jobs.csv"):
    """
    Crea un inventario de archivos .kjb y .ktr en un directorio dado.

    Args:
        directorio_principal (str): Ruta al directorio principal.
        archivo_salida (str, optional): Nombre del archivo de salida.
    """
    try:
        with open(archivo_salida, "w") as archivo:
            archivo.write(
                "Nombre,Ruta,Maquina,Extension,Tablas_Relacionadas, Base de datos, Source,Fecha_Modificacion, file_name\n")

            for root, dirs, files in os.walk(directorio_principal):
                for file in files:
                    # Filtro por extensión .kjb y .ktr
                    if file.endswith(".kjb") or file.endswith(".ktr"):
                        try:
                            ruta_completa = os.path.join(root, file)
                            directorio, nombre_archivo = os.path.split(
                                ruta_completa)

                            extension = os.path.splitext(file)[1]
                            fecha_modificacion = time.strftime(
                                '%Y-%m-%d %H:%M:%S', time.localtime(os.path.getmtime(ruta_completa)))
                            maquina = socket.gethostname()

                            # Extrae información del archivo XML
                            table_names, databases, source_uris, file_name = extraer_info_xml(
                                os.path.join(root, file))

                            table_names_str = '"' + \
                                ', '.join(table_names) + '"'
                            source_uris_str = '"' + \
                                ', '.join(source_uris) + '"'
                            database_str = '"' + ', '.join(databases) + '"'

                            # Escribe la información en el archivo CSV
                            archivo.write(f"{file},{directorio},{maquina},{extension},{table_names_str},{
                                          database_str}, {source_uris_str},{fecha_modificacion}, {file_name}\n")
                        except Exception as e:
                            print(f"Error al procesar el archivo {file}: {e}")
    except Exception as e:
        print(f"Error al abrir el archivo {archivo_salida}: {e}")


# Ejemplo de uso:
directorio_pentaho = r"C:\Users\pcolihui\Desktop\JOBS_PRD"
crear_inventario_kjb(directorio_pentaho)

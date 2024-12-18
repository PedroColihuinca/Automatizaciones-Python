import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
import time
import random
from datetime import datetime

# Configuración del logger
LOG_FILE = "trabajando_ofertas.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Configuración de salida
OUTPUT_FILE = "trabajando_jobs.csv"

# Función para guardar los datos en CSV


def save_to_csv(dataframe, file_name):
    """Guarda los datos en un archivo CSV."""
    try:
        dataframe.to_csv(file_name, index=False, sep=";", encoding="utf-8-sig")
        logging.info(f"Datos guardados exitosamente en {file_name}.")
    except Exception as e:
        logging.error(f"Error al guardar los datos en {file_name}: {str(e)}")

# Función para extraer las ofertas desde listadoOfertas


def scrape_listado_ofertas(base_url, max_ids=30):
    """Extrae ofertas de trabajo desde el listado principal."""
    logging.info("Iniciando extracción de listado de ofertas...")
    job_data = []

    for oferta_id in range(max_ids):
        try:
            # Construir la URL de cada oferta
            url = f"{base_url}/{oferta_id}"
            logging.info(f"Procesando oferta con ID: {
                         oferta_id} en URL: {url}")
            response = requests.get(url)
            if response.status_code != 200:
                logging.warning(f"No se pudo acceder a la página: {
                                url} (Status: {response.status_code})")
                continue

            soup = BeautifulSoup(response.content, "html.parser")
            detalle = soup.find("div", class_="detalleOfertaContainer")
            if not detalle:
                logging.warning(
                    f"No se encontró el detalle para la oferta con ID: {oferta_id}")
                continue

            # Extraer información
            title = detalle.find("h1").text.strip(
            ) if detalle.find("h1") else "N/A"
            company = detalle.find("a", class_="empresa").text.strip(
            ) if detalle.find("a", class_="empresa") else "N/A"
            location = detalle.find("span", class_="ubicacion").text.strip(
            ) if detalle.find("span", class_="ubicacion") else "N/A"
            description = detalle.find("div", class_="descripcion").text.strip(
            ) if detalle.find("div", class_="descripcion") else "N/A"

            # Guardar los datos de la oferta
            job_data.append({
                "ID Oferta": oferta_id,
                "Título": title,
                "Empresa": company,
                "Ubicación": location,
                "Descripción": description,
                "URL": url
            })

            # Delay entre solicitudes
            delay = random.uniform(2, 4)
            logging.info(f"Delay de {delay:.2f} segundos.")
            time.sleep(delay)

        except Exception as e:
            logging.error(f"Error al procesar la oferta con ID: {
                          oferta_id}: {str(e)}")

    logging.info(f"Se extrajeron {len(job_data)} ofertas de trabajo.")
    return pd.DataFrame(job_data)


# Main
if __name__ == "__main__":
    base_url = "https://www.trabajando.cl/listadoOfertas"
    job_dataframe = scrape_listado_ofertas(base_url, max_ids=30)

    if not job_dataframe.empty:
        save_to_csv(job_dataframe, OUTPUT_FILE)
    else:
        logging.warning("No se recopilaron datos de ofertas laborales.")

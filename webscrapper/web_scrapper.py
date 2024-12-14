import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
import re
import time
import random
from datetime import datetime
import lxml

# Configuración del logger
LOG_FILE = "trabajando_ti.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Configuración de salida
OUTPUT_FILE = "trabajando_ti_jobs.csv"
KEYWORDS = ["ingeniero", "informático", "analista",
            "desarrollador", "programador", "software", "datos", "TI"]

# Función para guardar los datos en CSV


def save_to_csv(dataframe, file_name):
    """Guarda los datos en un archivo CSV."""
    try:
        dataframe.to_csv(file_name, index=False, sep=";", encoding="utf-8-sig")
        logging.info(f"Datos guardados exitosamente en {file_name}.")
    except Exception as e:
        logging.error(f"Error al guardar los datos en {file_name}: {str(e)}")

# Función para extraer URLs del sitemap


def extract_ti_urls_from_sitemap(sitemap_url):
    """Extrae URLs relacionadas con el sector TI desde el sitemap."""
    try:
        response = requests.get(sitemap_url)
        if response.status_code != 200:
            logging.error(f"Error al obtener el sitemap: {
                          response.status_code}")
            return []

        # Usar lxml como parser
        soup = BeautifulSoup(response.content, "lxml-xml")
        urls = []
        for loc in soup.find_all("loc"):
            url_text = loc.text
            # Filtrar URLs relacionadas con el sector TI
            if any(keyword in url_text.lower() for keyword in KEYWORDS):
                urls.append(url_text)
        logging.info(f"Se encontraron {
                     len(urls)} URLs relacionadas con TI en el sitemap.")
        return urls
    except Exception as e:
        logging.error(f"Error al procesar el sitemap: {str(e)}")
        return []


# Función para extraer datos de una oferta de trabajo


def scrape_job_page(url):
    """Scrapea la información de una página de oferta de trabajo."""
    try:
        logging.info(f"Procesando URL: {url}")
        response = requests.get(url)
        if response.status_code != 200:
            logging.warning(f"No se pudo acceder a la página: {
                            url} (Status: {response.status_code})")
            return None

        soup = BeautifulSoup(response.content, "html.parser")
        title = soup.find("h1", class_="titulo-oferta")
        company = soup.find("a", class_="empresa")
        location = soup.find("span", class_="ubicacion")
        description = soup.find("div", class_="descripcion")

        # Extraer habilidades técnicas y experiencia
        description_text = description.text if description else ""
        skills = extract_technical_skills(description_text)
        experience_years = extract_experience_years(description_text)

        return {
            "Fuente": "Trabajando.cl",
            "Título": title.text.strip() if title else "N/A",
            "Empresa": company.text.strip() if company else "N/A",
            "Ubicación": location.text.strip() if location else "N/A",
            "Descripción": description_text.strip(),
            "Habilidades": ", ".join(skills),
            "Años de experiencia": experience_years,
            "URL": url,
        }
    except Exception as e:
        logging.error(f"Error al procesar la página {url}: {str(e)}")
        return None

# Funciones auxiliares para extraer habilidades y experiencia


def extract_technical_skills(description):
    """Extrae habilidades técnicas mencionadas en la descripción."""
    skills_regex = r'python|javascript|java|c\+\+|c#|php|ruby|swift|kotlin|go|rust|typescript|sql|aws|azure|gcp|docker|kubernetes'
    return list(set(re.findall(skills_regex, description.lower())))


def extract_experience_years(description):
    """Extrae los años de experiencia requeridos."""
    match = re.search(r'(\d+)[\s-]*años de experiencia', description.lower())
    return int(match.group(1)) if match else None

# Función principal de scraping


def scrape_trabajando_ti(sitemap_url, max_urls=20):
    """Scrapea ofertas laborales del sector TI en Trabajando.cl."""
    logging.info("Iniciando scraping en Trabajando.cl (sector TI)...")
    urls = extract_ti_urls_from_sitemap(sitemap_url)[:max_urls]
    job_data = []

    for url in urls:
        job = scrape_job_page(url)
        if job:
            job_data.append(job)

        # Introducir un delay aleatorio para no sobrecargar el servidor
        delay = random.uniform(2, 5)  # Entre 2 y 5 segundos
        logging.info(f"Delay de {delay:.2f} segundos.")
        time.sleep(delay)

    if job_data:
        df = pd.DataFrame(job_data)
        save_to_csv(df, OUTPUT_FILE)
    else:
        logging.warning(
            "No se recopilaron datos de ofertas laborales relacionadas con TI.")
    logging.info("Proceso de scraping finalizado.")


# Main
if __name__ == "__main__":
    sitemap_url = "https://www.trabajando.cl/sitemap-ofertas.xml"
    scrape_trabajando_ti(sitemap_url, max_urls=20)

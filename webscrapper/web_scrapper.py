import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import logging
from datetime import datetime
import re
from urllib.parse import quote
import time
import random


class TrabajandoClScraperPlaywright:
    def __init__(self, search_terms, pages=1):
        self.search_terms = [quote(term) for term in search_terms]
        self.pages = pages
        self.jobs_data = []
        self.setup_logging()

    def setup_logging(self):
        """Configura el registro de logs."""
        logging.basicConfig(
            filename=f'trabajando_scraper_{
                datetime.now().strftime("%Y%m%d")}.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        logging.info("Iniciando el scraper de Trabajando.cl")

    def extract_technical_skills(self, description):
        """Extrae habilidades técnicas mencionadas en la descripción."""
        languages = r'python|javascript|java|c\+\+|c#|php|ruby|swift|kotlin|go|rust|typescript'
        databases = r'mysql|postgresql|mongodb|oracle|sql server|sqlite|redis|elasticsearch'
        clouds = r'aws|azure|gcp|google cloud|heroku|digitalocean'
        tools = r'looker studio|power bi|tableau|kubernetes|jira|git'

        skills = {
            "languages": list(set(re.findall(languages, description.lower()))),
            "databases": list(set(re.findall(databases, description.lower()))),
            "clouds": list(set(re.findall(clouds, description.lower()))),
            "tools": list(set(re.findall(tools, description.lower())))
        }
        return skills

    def extract_experience_years(self, description):
        """Extrae los años de experiencia requeridos de la descripción."""
        patterns = [
            r'(\d+)[\s-]*años de experiencia',
            r'experiencia[:\s]*(\d+)[\s-]*años',
            r'(\d+)[\s-]*years of experience',
            r'experience[:\s]*(\d+)[\s-]*years'
        ]
        for pattern in patterns:
            match = re.search(pattern, description.lower())
            if match:
                return int(match.group(1))
        return None

    async def scrape_trabajandocl(self):
        """Realiza el scraping de Trabajando.cl usando Playwright."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            page = await context.new_page()

            try:
                for term in self.search_terms:
                    for page_num in range(self.pages):
                        url = f"https://www.trabajando.cl/empleos?q={
                            term}&pagina={page_num+1}"
                        print(f"Accediendo a {url}...")

                        # Navegar a la página
                        await page.goto(url, wait_until='networkidle')
                        await page.wait_for_timeout(random.randint(2000, 4000))

                        # Scroll suave
                        await self._scroll_page(page)

                        # Extraer trabajos
                        jobs = await page.query_selector_all('.joblist__item')

                        if not jobs:
                            print(f"No se encontraron trabajos para {
                                  term} en la página {page_num + 1}")
                            continue

                        for job in jobs:
                            try:
                                job_data = await self._extract_job_data(job)
                                if job_data:
                                    self.jobs_data.append(job_data)
                            except Exception as e:
                                logging.error(
                                    f"Error procesando trabajo: {str(e)}")

                        await page.wait_for_timeout(random.randint(4000, 8000))

            except Exception as e:
                logging.error(f"Error general en el scraping: {str(e)}")
                print(f"Error: {str(e)}")
            finally:
                await browser.close()

    async def _scroll_page(self, page):
        """Realiza scroll suave en la página."""
        await page.evaluate("""
            async () => {
                await new Promise((resolve) => {
                    let totalHeight = 0;
                    const distance = 100;
                    const timer = setInterval(() => {
                        const scrollHeight = document.body.scrollHeight;
                        window.scrollBy(0, distance);
                        totalHeight += distance;
                        
                        if(totalHeight >= scrollHeight){
                            clearInterval(timer);
                            resolve();
                        }
                    }, 100);
                });
            }
        """)
        await page.wait_for_timeout(1000)

    async def _extract_job_data(self, job):
        """Extrae datos de una oferta de trabajo individual."""
        try:
            title = await job.query_selector('.joblist__item-title')
            company = await job.query_selector('.joblist__item-company')
            location = await job.query_selector('.joblist__item-location')
            description_link = await job.query_selector('.joblist__item-title a')

            # Si encontramos un enlace, navegamos a la página de descripción
            description_url = await description_link.get_attribute('href') if description_link else None

            title_text = await title.text_content() if title else "No disponible"
            company_text = await company.text_content() if company else "No disponible"
            location_text = await location.text_content() if location else "No disponible"

            # Descripción detallada
            description_text = "No disponible"
            if description_url:
                # Abrir nueva página para obtener descripción completa
                context = page.context
                description_page = await context.new_page()
                await description_page.goto(description_url, wait_until='networkidle')

                description_element = await description_page.query_selector('.job-detail__description')
                description_text = await description_element.text_content() if description_element else "No disponible"

                await description_page.close()

            # Extracción de habilidades técnicas
            skills = self.extract_technical_skills(description_text)
            experience_years = self.extract_experience_years(description_text)

            return {
                'title': title_text.strip(),
                'company': company_text.strip(),
                'location': location_text.strip(),
                'description': description_text.strip(),
                'languages': ", ".join(skills['languages']),
                'databases': ", ".join(skills['databases']),
                'clouds': ", ".join(skills['clouds']),
                'tools': ", ".join(skills['tools']),
                'experience_years': experience_years,
                'scraped_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        except Exception as e:
            logging.error(f"Error extrayendo datos del trabajo: {str(e)}")
            return None

    def save_to_csv(self):
        """Guarda los datos en un archivo CSV con delimitador ;."""
        if not self.jobs_data:
            logging.warning("No se encontraron datos para guardar.")
            print("No se encontraron datos para guardar.")
            return

        df = pd.DataFrame(self.jobs_data)
        output_file = f'trabajando_jobs_chile_{
            datetime.now().strftime("%Y%m%d")}.csv'
        df.to_csv(output_file, index=False, sep=';', encoding='utf-8-sig')
        logging.info(f"Datos guardados en {output_file}")
        print(f"Datos guardados en {output_file}")

    async def run(self):
        """Ejecuta el scraper."""
        print("Iniciando scraping en Trabajando.cl para Chile...")
        logging.info("Iniciando scraping en Trabajando.cl para Chile")
        await self.scrape_trabajandocl()
        self.save_to_csv()
        print("Scraping completado!")
        logging.info("Scraping completado")


if __name__ == "__main__":
    search_terms = [
        "ingeniero informático",
        "ingeniero de software",
        "analista de software",
        "analista de datos",
        "ingeniero de datos"
    ]

    scraper = TrabajandoClScraperPlaywright(
        search_terms=search_terms,
        pages=2
    )
    asyncio.run(scraper.run())

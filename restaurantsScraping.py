import boto3
import json
import time
import logging
import random

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from uuid import uuid4



dynamodb = boto3.resource('dynamodb', region_name="eu-west-3")
restaurants_table = dynamodb.Table("Restaurants-dev")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TRIPADVISOR_URL = "https://www.tripadvisor.com/Restaurants-g187147-Paris_Ile_de_France.html"

def setup_driver():
    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    return driver

def scrape_tripadvisor_restaurants():
    logger.info("🚀 Démarrage du scraping TripAdvisor...")

    driver = setup_driver()
    driver.get(TRIPADVISOR_URL)
    time.sleep(5)

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'XIWnB z y')]"))
        )
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(random.uniform(3, 6))
    except Exception as e:
        logger.error(f"❌ Les éléments n'ont pas été trouvés : {e}")
        driver.quit()
        return []

    restaurants = []

    biz_elements = driver.find_elements(By.XPATH, "//div[contains(@class, 'XIWnB z y')]")[:10]  # ✅ Limiter à 10 restaurants

    logger.info(f"✅ {len(biz_elements)} restaurants trouvés.")

    for biz in biz_elements:
        try:
            restaurant_id = str(uuid4())
            name = biz.find_element(By.XPATH, ".//a[contains(text(), '')]").text
            link_element = biz.find_element(By.TAG_NAME, "a")
            link = link_element.get_attribute("href")
            restaurants_table.put_item(
                Item={
                    "restaurants_id": restaurant_id,
                    "name": name,
                    "link": link,
                }
            )
            restaurants.append({"restaurans_id": restaurant_id, "name": name, "link": link})

        except Exception as e:
            logger.error(f"Erreur lors de l'extraction des données : {e}")

    driver.quit()
    return restaurants

if __name__ == "__main__":
    scraped_restaurants = scrape_tripadvisor_restaurants()
    print("🔹 Restaurants récupérés:", json.dumps(scraped_restaurants, indent=4, ensure_ascii=False))
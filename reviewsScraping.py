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

#Initialisation de DynamoDB
dynamodb = boto3.resource('dynamodb', region_name="eu-west-3")
restaurants_table = dynamodb.Table("Restaurants-dev")
reviews_table = dynamodb.Table("Reviews-dev")

#Configuration des logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

def get_restaurants():
    response = restaurants_table.scan()
    return response.get("Items", [])

def scrape_reviews_for_restaurant(restaurant):
    driver = setup_driver()
    restaurant_id = restaurant["restaurants_id"]
    restaurant_link = restaurant["link"]

    logger.info(f"🚀 Scraping des avis pour {restaurant['name']}...")

    driver.get(restaurant_link)
    time.sleep(5)

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//div[@data-automation='reviewCard']"))
        )
    except Exception as e:
        logger.error(f"❌ Aucun avis trouvé pour {restaurant['name']} : {e}")
        driver.quit()
        return []

    reviews = []
    
    review_elements = driver.find_elements(By.XPATH, "//div[@data-automation='reviewCard']")[:10]
    logger.info(f"✅ {len(review_elements)} restaurants trouvés.")

    for review in review_elements:
        try:
            review_id = str(uuid4())

            try:
                text = review.find_element(By.XPATH, ".//span[contains(@class, 'JguWG')]").text
            except:
                text = "Pas de commentaire"

            try:
                rating_svg = review.find_element(By.XPATH, "//*[name()='svg']/*[name()='title']").text
                rating = rating_svg.split(" ")[0]
            except Exception as e:
                rating = "Non noté"
                logger.error(f"Erreur récupération note: {e}")

            try:
                visit_type = review.find_element(By.XPATH, ".//span[contains(@class, 'DlAxN')]").text
            except Exception as e:
                visit_type = "Non spécifié"
                logger.error(f"Erreur récupération type de visite: {e}")

            reviews_table.put_item(
                Item={
                    "reviews_id": review_id,
                    "restaurants_id": restaurant_id, 
                    "text": text,
                    "rating": rating,
                    "visit_type": visit_type,
                }
            )

            reviews.append({
                "reviews_id": review_id,
                "restaurants_id": restaurant_id,
                "text": text,
                "rating": rating,
                "visit_type": visit_type,
            })

        except Exception as e:
            logger.error(f"Erreur lors de l'extraction d'un avis : {e}")

    driver.quit()
    return reviews

def scrape_tripadvisor_reviews():
    restaurants = get_restaurants()
    
    if not restaurants:
        logger.error("🚨 Aucun restaurant trouvé dans la base de données !")
        return []

    all_reviews = []
    for restaurant in restaurants:
        reviews = scrape_reviews_for_restaurant(restaurant)
        all_reviews.extend(reviews)

    return all_reviews

if __name__ == "__main__":
    scraped_reviews = scrape_tripadvisor_reviews()
    print("🔹 Avis récupérés:", json.dumps(scraped_reviews, indent=4, ensure_ascii=False))
# =====================================================
# HYATT PET HOTELS SCRAPER (SIMPLE LOGIC, FULL FEATURES)
# =====================================================

import os
import re
import csv
import json
import time
import random
from datetime import datetime
from config import Hyattconfig as config
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
    WebDriverException,
)

# =====================================================
# CONFIG
# =====================================================

START_URL = "https://www.hyatt.com/landing/promo/pet-friendly-hotels-at-hyatt"

HEADLESS = False
TIMEOUT = 40
HUMAN_SLEEP = 1.5

OUTPUT_JSON = config.OUTPUT_JSON
OUTPUT_CSV = config.OUTPUT_CSV
CHECKPOINT_JSON = config.CHECKPOINT_JSON

FIELDS = [
    "hotel_code",
    "hotel_name",
    "address_line_1",
    "address_line_2",
    "description",
    "amenities_json",
    "pet_policy_description",
    "pet_fees_json",
    "weight_limits_json",
    "phone",
    "hotel_url",
    "last_updated",
]

# =====================================================
# SELECTORS
# =====================================================

SEL_CARD = "div.styles_hotel-card__content__VYOVM"
SEL_NAME = "a.be-text-card-title"
SEL_ADDR1 = "div.styles_hotel-card__address-1__wFSlx"
SEL_ADDR2 = "div.styles_hotel-card__address-2__cBaYR"
SEL_SHOW_MORE = "div.styles_hotel-gallery-list__more-btn__oBihI button"
SEL_COUNT = ".styles_location-count__gYFta"

XPATH_DESCRIPTION = '//*[@id="__next"]/main/div/div[3]/div/div[1]/div[2]/p'
SEL_AMENITIES = 'ul[data-locator="amenity-list-core2"] li p'
SEL_PETS_OVERVIEW = 'div[data-locator="pets-overview-text"]'
SEL_PET_FEES = '[data-locator="pet-policy-fees"]'
SEL_WEIGHT = 'p[data-locator*="weight"]'

# =====================================================
# UTILS
# =====================================================

def safe_text(el):
    try:
        return el.text.strip()
    except:
        return None

def now():
    return datetime.utcnow().isoformat()

def parse_hotel_code(url):
    try:
        slug = url.split("/")[-1]
        return slug.split("-")[0].lower()
    except:
        return None

# =====================================================
# CHECKPOINT
# =====================================================

class Checkpoint:
    def __init__(self, path):
        self.path = path
        self.index = 0
        self.load()

    def load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    self.index = json.load(f).get("last_hotel_index", 0)
            except:
                pass

    def save(self, idx):
        self.index = idx
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump({"last_hotel_index": idx}, f, indent=2)

    def clear(self):
        self.save(0)

# =====================================================
# DRIVER
# =====================================================

def create_driver():
    options = uc.ChromeOptions()
    if HEADLESS:
        options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    driver = uc.Chrome(options=options)
    driver.set_page_load_timeout(60)
    return driver

# =====================================================
# DETAIL SCRAPER
# =====================================================

def scrape_detail(driver, url):
    out = {
        "description": None,
        "amenities_json": None,
        "pet_policy_description": None,
        "pet_fees_json": None,
        "weight_limits_json": None,
        "phone": None,
    }

    main = driver.current_window_handle
    driver.switch_to.new_window("tab")
    driver.get(url)
    time.sleep(2)

    try:
        try:
            desc = driver.find_element(By.XPATH, XPATH_DESCRIPTION)
            out["description"] = safe_text(desc)
        except:
            pass

        amenities = []
        for a in driver.find_elements(By.CSS_SELECTOR, SEL_AMENITIES):
            t = safe_text(a)
            if t:
                amenities.append(t)
        if amenities:
            out["amenities_json"] = amenities

        pet_desc = None
        for p in driver.find_elements(By.CSS_SELECTOR, SEL_PETS_OVERVIEW):
            t = safe_text(p)
            if t and (not pet_desc or len(t) > len(pet_desc)):
                pet_desc = t
        out["pet_policy_description"] = pet_desc

        fees = []
        for f in driver.find_elements(By.CSS_SELECTOR, SEL_PET_FEES):
            t = safe_text(f)
            if t:
                fees.append(t)
        if fees:
            out["pet_fees_json"] = fees

        weights = []
        for w in driver.find_elements(By.CSS_SELECTOR, SEL_WEIGHT):
            t = safe_text(w)
            if t:
                weights.append(t)
        if weights:
            out["weight_limits_json"] = weights

        for a in driver.find_elements(By.CSS_SELECTOR, 'a[href^="tel:"]'):
            out["phone"] = a.get_attribute("href").replace("tel:", "")
            break

    finally:
        driver.close()
        driver.switch_to.window(main)
        time.sleep(1)

    return out

# =====================================================
# MAIN
# =====================================================

def main():
    checkpoint = Checkpoint(CHECKPOINT_JSON)

    existing = []
    if os.path.exists(OUTPUT_JSON):
        with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
            existing = json.load(f)

    scraped_urls = {h["hotel_url"] for h in existing if h.get("hotel_url")}
    results = existing[:]

    driver = create_driver()
    wait = WebDriverWait(driver, TIMEOUT)

    driver.get(START_URL)
    time.sleep(3)

    hotel_index = 0

    try:
        while True:
            cards = driver.find_elements(By.CSS_SELECTOR, SEL_CARD)
            total_cards = len(cards)

            while hotel_index < total_cards:
                try:
                    card = cards[hotel_index]

                    name_el = card.find_element(By.CSS_SELECTOR, SEL_NAME)
                    hotel_url = name_el.get_attribute("href")

                    if hotel_url in scraped_urls:
                        hotel_index += 1
                        checkpoint.save(hotel_index)
                        continue

                    record = {
                        "hotel_code": parse_hotel_code(hotel_url),
                        "hotel_name": safe_text(name_el),
                        "address_line_1": safe_text(card.find_element(By.CSS_SELECTOR, SEL_ADDR1)),
                        "address_line_2": safe_text(card.find_element(By.CSS_SELECTOR, SEL_ADDR2)),
                        "hotel_url": hotel_url,
                        "last_updated": now(),
                    }

                    print(f"\n▶ [{hotel_index}] {record['hotel_name']}")

                    detail = scrape_detail(driver, hotel_url)
                    record.update(detail)

                    results.append(record)
                    scraped_urls.add(hotel_url)

                    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
                        json.dump(results, f, indent=2, ensure_ascii=False)

                    hotel_index += 1
                    checkpoint.save(hotel_index)

                    time.sleep(HUMAN_SLEEP)

                except (StaleElementReferenceException, NoSuchElementException):
                    cards = driver.find_elements(By.CSS_SELECTOR, SEL_CARD)
                    continue

            # try loading more
            try:
                btn = driver.find_element(By.CSS_SELECTOR, SEL_SHOW_MORE)
                driver.execute_script(
                    "arguments[0].scrollIntoView({block:'center'});", btn
                )
                time.sleep(1)
                btn.click()
                print("⬇ SHOW MORE")
                time.sleep(2)
            except:
                print("✅ NO MORE BUTTON")
                break


    finally:
        driver.quit()
        checkpoint.clear()

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        for r in results:
            writer.writerow({k: r.get(k) for k in FIELDS})

    print(f"\n🎉 DONE — TOTAL HOTELS: {len(results)}")

if __name__ == "__main__":
    main()

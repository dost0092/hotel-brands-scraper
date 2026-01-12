
import time
import re
import csv
import json
import os
from datetime import datetime

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
    NoSuchElementException
)

# ================== CONFIG ==================

START_URL = "https://www.hilton.com/en/locations/pet-friendly/"
JSON_OUTPUT_DIR = r"D:\Personal\KRUIZ\KRUIZ\hotel_brands_screper\json\hilton"
os.makedirs(JSON_OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE_JSON = os.path.join(
    JSON_OUTPUT_DIR,
    "hilton_pet_friendly_hotels.json"
)
CSV_OUTPUT_DIR = r"D:\Personal\KRUIZ\KRUIZ\hotel_brands_screper\csv\hilton"
os.makedirs(CSV_OUTPUT_DIR, exist_ok=True)

OUTPUT_FILE_CSV = os.path.join(
    CSV_OUTPUT_DIR,
    "hilton_pet_friendly_hotels.csv"
)

STATE_FILE = "hilton_last_state.json"

FIELDS = [
    "hotel_code",
    "hotel_name",
    "address",
    "address_map_url",  # 🆕 added here
    "phone",
    "rating",
    "description",
    "card_price",
    "overview_table_json",
    "pets_json",
    "parking_json",
    "amenities_json",
    "nearby_json",
    "airport_json",
    "is_pet_friendly",
    "state",  # 🆕 add state
    "country",  # 🆕 add country
    "last_updated",
    "property_url"
]

MAX_SCROLLS = 20
RETRY_LIMIT = 3
HEADLESS = False 
LOCATIONS_FILE = "D:\Personal\KRUIZ\KRUIZ\hotel_brands_screper\json\pagination_links/hilton_locations.json"


# 🔒 WATCHDOG CONFIG (ADDED)
STALL_TIMEOUT_SECONDS = 20000  # 3 minutes
WATCHDOG_CHECK_INTERVAL = 20

# ================== GLOBAL WATCHDOG ==================
last_success_time = time.time()
REFRESH_EVERY_PAGES = 1


# ================== UTILS ==================

def make_options():
    opts = uc.ChromeOptions()
    if HEADLESS:
        uc_options = uc.ChromeOptions()
        uc_options.add_argument("--headless=new")
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    return opts


def load_locations():
    with open(LOCATIONS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def click_pet_friendly_filter(driver, wait):
    try:
        btn = wait.until(EC.element_to_be_clickable((
            By.XPATH,
            "//button[contains(@aria-label,'Pet-Friendly')]"
        )))
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(3)
        print("🐾 Pet-Friendly filter applied")
    except Exception as e:
        print("⚠ Pet-Friendly filter not found / already applied")


def extract_money(text):
    if not text:
        return ""
    m = re.search(r"([$€£R$]\s?\d+[.,]?\d*)", text)
    return m.group(1) if m else ""


def extract_weight(text):
    if not text:
        return ""
    m = re.search(r"(\d+\s?(lb|kg))", text.lower())
    return m.group(1) if m else ""


def save_state(location_idx, page, hotel_idx):
    with open(STATE_FILE, "w") as f:
        json.dump({
            "location_idx": location_idx,
            "page": page,
            "hotel_idx": hotel_idx
        }, f)



def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            data = json.load(f)

        return {
            "location_idx": data.get("location_idx", data.get("last_location_index", 0)),
            "page": data.get("page", max(1, data.get("last_page", 1))),
            "hotel_idx": data.get("hotel_idx", data.get("last_card_index", 0))
        }

    return {
        "location_idx": 0,
        "page": 1,
        "hotel_idx": 0
    }




def wait_for_popup_content(driver, timeout=40):
    start = time.time()
    while True:
        try:
            popup = driver.find_element(
                By.CSS_SELECTOR,
                "div.relative.flex.size-full.flex-col.overflow-y-auto"
            )
            text_nodes = [e for e in popup.find_elements(By.XPATH, ".//*") if e.text.strip()]
            if len(text_nodes) > 8:
                return popup
        except:
            pass
        if time.time() - start > timeout:
            raise TimeoutException("Popup content did not load")
        time.sleep(0.4)


def safe_find_text(el, xpath):
    try:
        return el.find_element(By.XPATH, xpath).text.strip()
    except:
        return ""


def parse_overview_table(popup):
    data = {}
    try:
        rows = popup.find_elements(By.XPATH, ".//table//tr")
        for row in rows:
            try:
                key = row.find_element(By.XPATH, ".//th").text.strip()
                val = row.find_element(By.XPATH, ".//td").text.strip()
                data[key] = val
            except:
                continue
    except:
        pass
    return data


def parse_amenities(popup):
    amenities = []
    try:
        li_elements = popup.find_elements(
            By.XPATH, ".//ul[contains(@class,'peer flex')]/li"
        )
        for li in li_elements:
            label = safe_find_text(li, ".//span[@data-testid='hotelAmenityLabel']")
            if label:
                amenities.append(label)
    except:
        pass
    return amenities


def parse_nearby(popup):
    data = []
    try:
        items = popup.find_elements(By.XPATH, "//*[@id='tab-panel-nearBy']//li")
        for item in items:
            try:
                place = safe_find_text(item, ".//div[1]/span")
                distance = safe_find_text(item, ".//div[2]")
                if place:
                    data.append({"place": place, "distance": distance})
            except:
                continue
    except:
        pass
    return data


def parse_airport_info(popup):
    data = []
    try:
        btn = popup.find_element(By.XPATH, "//*[@id='airport']")
        btn.click()
        time.sleep(1)
        items = popup.find_elements(By.XPATH, "//*[@id='tab-panel-airport']//li")
        for item in items:
            try:
                name = safe_find_text(item, ".//div[1]/div/span[last()]")
                distance = safe_find_text(item, ".//div[1]/div[2]")
                shuttle = safe_find_text(item, ".//p")
                if name:
                    data.append({"airport": name, "distance": distance, "shuttle": shuttle})
            except:
                continue
    except:
        pass
    return data


def retry_action(action, retries=RETRY_LIMIT, delay=2):
    for i in range(retries):
        try:
            return action()
        except Exception as e:
            print(f"⚠ Retry {i+1}/{retries}: {e}")
            time.sleep(delay)
    raise Exception("Max retries exceeded")


# ================== MAIN SCRAPER ==================

def main():
    global last_success_time

    state = load_state()
    start_location_idx = state["location_idx"]
    start_page = state["page"]
    start_hotel_idx = state["hotel_idx"]

    print(
        f"🔄 Resuming from location {start_location_idx}, "
        f"page {start_page}, hotel {start_hotel_idx}"
    )


    driver = uc.Chrome(options=make_options(), use_subprocess=True)
    wait = WebDriverWait(driver, 60)
    hotels = []
    page = start_page

    if not os.path.exists(OUTPUT_FILE_CSV):
        with open(OUTPUT_FILE_CSV, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=FIELDS).writeheader()

    if not os.path.exists(OUTPUT_FILE_JSON):
        with open(OUTPUT_FILE_JSON, "w", encoding="utf-8") as f:
            json.dump([], f)

    try:
        locations = load_locations()

        for loc_idx, loc in enumerate(locations):
            if loc_idx < start_location_idx:
                continue

            location_name = loc["location_name"]
            location_url = loc["url"]

            print(f"\n🌍 Scraping location: {location_name}")
            driver.get(location_url)

            wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
            time.sleep(3)

            # ✅ CLICK PET-FRIENDLY FILTER (ONCE PER LOCATION)
            try:
                pet_btn = wait.until(EC.element_to_be_clickable((
                    By.XPATH,
                    "//button[starts-with(@aria-label,'Pet-Friendly')]"
                )))

                driver.execute_script(
                    "arguments[0].scrollIntoView({block:'center'});", pet_btn
                )
                time.sleep(0.5)

                driver.execute_script("arguments[0].click();", pet_btn)
                time.sleep(3)

                print("🐾 Pet-Friendly filter applied")

            except Exception as e:
                print(f"⚠ Pet-Friendly filter not found / already active: {e}")

            if loc_idx == start_location_idx:
                page = start_page
            else:
                page = 1
                start_hotel_idx = 0


            while True:
                print(f"📄 Scraping page {page} ({location_name})...")

                # 🛡 WATCHDOG CHECK
                if time.time() - last_success_time > STALL_TIMEOUT_SECONDS:
                    raise TimeoutException("Scraping stalled — restarting browser")

                buttons = driver.find_elements(
                    By.XPATH,
                    "//button[.//span[normalize-space()='View hotel details']]"
                )

                for i, btn in enumerate(buttons):
                    if (
                        loc_idx == start_location_idx
                        and page == start_page
                        and i < start_hotel_idx
                    ):
                        continue
                    try:
                        # 🆕 Extract property website (Visit website link)
                        try:
                            property_url = popup.find_element(
                                By.XPATH,
                                ".//a[contains(@href,'hilton.com/en/hotels/') and contains(text(),'Visit website')]"
                            ).get_attribute("href")
                        except:
                            property_url = ""

                        # 🆕 Extract map link (Google Maps “Directions” link)
                        try:
                            address_map_url = popup.find_element(
                                By.XPATH,
                                ".//a[contains(@href,'https://www.google.com/maps/search/?api=1')]"
                            ).get_attribute("href")
                        except:
                            address_map_url = ""

                        driver.execute_script(
                            "arguments[0].scrollIntoView({block:'center'});", btn
                        )
                        driver.execute_script(
                            "arguments[0].scrollIntoView({block:'center'});", btn
                        )
                        time.sleep(0.5)
                        driver.execute_script("arguments[0].click();", btn)

                        popup = retry_action(lambda: wait_for_popup_content(driver))
                        all_text = "\n".join(
                            e.text.strip()
                            for e in popup.find_elements(By.XPATH, ".//*")
                            if e.text.strip()
                        )

                        name = safe_find_text(popup, ".//h1 | .//h2") or "UNKNOWN"
                        rating = safe_find_text(popup, ".//p[contains(text(),'Rating')]")
                        description = safe_find_text(
                            popup, ".//div/p[@class='inline text-start md:block']"
                        )
                        address = safe_find_text(driver, ".//span[@data-testid='locationMarker']")
                        price = safe_find_text(driver, ".//span[@data-testid='rateItem']")

                        overview = parse_overview_table(popup)
                        amenities = parse_amenities(popup)
                        nearby = parse_nearby(popup)
                        airport = parse_airport_info(popup)

                        pets_json = {k: v for k, v in overview.items() if "pet" in k.lower()}
                        parking_json = {k: v for k, v in overview.items() if "park" in k.lower()}
                        print(pets_json)
                        hotel_data = {
                            "hotel_code": f"HILTON-{location_name}-{page}-{i+1}",
                            "hotel_name": name,
                            "property_url": property_url,
                            "address": address,
                            "address_map_url": address_map_url,   # 🆕 Added line
                            "phone": re.search(
                                r'(\+?\d[\d\s().-]{7,}\d)', all_text
                            ).group(1) if re.search(
                                r'(\+?\d[\d\s().-]{7,}\d)', all_text
                            ) else "",
                            "rating": rating,
                            "description": description,
                            "card_price": price,
                            "overview_table_json": json.dumps(overview, ensure_ascii=False),
                            "pets_json": json.dumps(pets_json, ensure_ascii=False),
                            "parking_json": json.dumps(parking_json, ensure_ascii=False),
                            "amenities_json": json.dumps(amenities, ensure_ascii=False),
                            "nearby_json": json.dumps(nearby, ensure_ascii=False),
                            "airport_json": json.dumps(airport, ensure_ascii=False),
                            "is_pet_friendly": "true",
                            "state": location_name,          # 🆕
                            "country": "USA",                # 🆕
                            "last_updated": datetime.utcnow().isoformat()
                        }

                        last_success_time = time.time()
                        print(f"✅ Extracted: {hotel_data['hotel_name']}")

                        # ✅ Save after each record (CSV + JSON + STATE)
                        with open(OUTPUT_FILE_CSV, "a", newline="", encoding="utf-8") as f:
                            csv.DictWriter(f, fieldnames=FIELDS).writerow(hotel_data)

                        # JSON append safely after each record
                        with open(OUTPUT_FILE_JSON, "r+", encoding="utf-8") as jf:
                            try:
                                data = json.load(jf)
                            except json.JSONDecodeError:
                                data = []
                            data.append(hotel_data)
                            jf.seek(0)
                            json.dump(data, jf, ensure_ascii=False, indent=2)
                            jf.truncate()

                        save_state(loc_idx, page, i + 1)

                        # Close popup safely
                        popup.send_keys(Keys.ESCAPE)
                        time.sleep(1)


                    except Exception:
                        try:
                            driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
                        except:
                            pass
                        continue

                try:
                    btn_next = driver.find_element(By.ID, "pagination-right")

                    if not btn_next.is_enabled() or "disabled" in btn_next.get_attribute("class"):
                        print(f"✅ Finished all pages for {location_name}")
                        break  # exit the while loop properly

                    # go to next page
                    driver.execute_script("arguments[0].click();", btn_next)
                    page += 1
                    time.sleep(4)
                    save_state(loc_idx, page, 0)

                    # periodic refresh (unchanged logic)
                    if page % REFRESH_EVERY_PAGES == 0:
                        print("🔄 Refreshing browser to avoid stale state...")
                        driver.refresh()
                        wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
                        time.sleep(3)
                        click_pet_friendly_filter(driver, wait)
                        for _ in range(page - 1):
                            try:
                                driver.execute_script(
                                    "arguments[0].click();",
                                    wait.until(EC.element_to_be_clickable((By.ID, "pagination-right")))
                                )
                                time.sleep(3)
                            except:
                                break
                except NoSuchElementException:
                    print(f"✅ No pagination found, moving to next location ({location_name})")
                    break
            
            # ✅ ADD THIS RIGHT HERE (IMPORTANT)
            print(f"➡ Finished location: {location_name}")
            save_state(loc_idx + 1, 1, 0)

            # reset resume pointers for next location
            start_page = 1
            start_hotel_idx = 0

    finally:
        driver.quit()



if __name__ == "__main__":
    while True:
        try:
            print("\n🚀 Script starting...")
            main()
        except Exception as e:
            print(f"\n❌ Error occurred: {e}")
            print("🔁 Restarting in 10 seconds...\n")
            time.sleep(10)
            continue



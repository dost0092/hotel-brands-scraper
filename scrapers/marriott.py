import time
import re
import csv
import json
import os
from datetime import datetime
from urllib.parse import urljoin
from config import MarriottConfig as config
# ================== PYTHON 3.12 COMPATIBILITY PATCH ==================
import sys

if sys.version_info >= (3, 12):
    import types
    import re as _re

    distutils = types.ModuleType("distutils")
    version_mod = types.ModuleType("distutils.version")

    class LooseVersion:
        def __init__(self, v):
            self.vstring = str(v)         # used by undetected_chromedriver
            self.version = tuple(int(x) if x.isdigit() else x for x in _re.split(r'[._-]', str(v)))

        def __str__(self):
            return self.vstring

        def __repr__(self):
            return f"LooseVersion('{self.vstring}')"

        def __lt__(self, other): return self.version < other.version
        def __le__(self, other): return self.version <= other.version
        def __eq__(self, other): return self.version == other.version
        def __ge__(self, other): return self.version >= other.version
        def __gt__(self, other): return self.version > other.version

    version_mod.LooseVersion = LooseVersion
    distutils.version = version_mod

    sys.modules["distutils"] = distutils
    sys.modules["distutils.version"] = version_mod
# ======================================================================

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
    NoSuchElementException,
    ElementClickInterceptedException,
    ElementNotInteractableException,
)

# ================== CONFIG ==================
START_URL = config.START_URL
OUTPUT_FILE_CSV = config.OUTPUT_FILE_CSV
OUTPUT_FILE_JSON = config.OUTPUT_FILE_JSON
OUTPUT_REGIONS_JSON =config.OUTPUT_REGIONS_JSON
STATE_FILE = config.STATE_FILE
# ================== TEST MODE ==================
TEST_MODE = False        # True = test limited cards | False = full run
TEST_CARD_LIMIT = 10    # number of cards to scrape in test mode
MAX_CARD_RETRIES = 5

FIELDS = [
    "hotel_code",
    "hotel_name",
    "address",
    "city",
    "state",
    "country",
    "phone",
    "rating",
    "description",
    "card_price",
    "property_website",
    "overview_table_json",
    "pets_json",
    "parking_json",
    "amenities_json",
    "nearby_json",
    "airport_json",
    "is_pet_friendly",
    "last_updated",
]

RETRY_LIMIT = 3
WATCHDOG_STALL_SECONDS = 290  # restart if no success for 3 minutes
WATCHDOG_CHECK_INTERVAL = 10

last_success_time = time.time()


# ================== HELPERS ==================

def make_options():
    opts = uc.ChromeOptions()
    # Explicitly not headless
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-infobars")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    return opts


def wait_for(driver, cond, timeout=40, poll_frequency=0.2):
    return WebDriverWait(driver, timeout, poll_frequency).until(cond)

def get_card_identity(card):
    """
    Returns a stable identifier for a hotel card.
    Prefer hotel name; fallback to text hash.
    """
    try:
        name = safe_text(card.find_element(By.CSS_SELECTOR, ".t-subtitle-xl"))
        if name:
            return name.strip().lower()
    except:
        pass

    return safe_text(card).strip().lower()[:120]

def js_click(driver, el):
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    driver.execute_script("arguments[0].click();", el)

def parse_property_website(modal_root):
    """
    Extracts 'View Property Website' link from the hotel modal
    """
    try:
        link_el = modal_root.find_element(
            By.CSS_SELECTOR,
            "a.title-container__category-box[href]"
        )
        href = link_el.get_attribute("href")
        if href:
            return urljoin("https://www.marriott.com", href)
    except:
        pass
    return ""

def safe_text(el):
    try:
        return el.text.strip()
    except:
        return ""


def retry(action, retries=RETRY_LIMIT, delay=1.5):
    last_exc = None
    for i in range(retries):
        try:
            return action()
        except Exception as e:
            last_exc = e
            time.sleep(delay)
    if last_exc:
        raise last_exc


def ensure_output_files():
    if not os.path.exists(OUTPUT_FILE_CSV):
        with open(OUTPUT_FILE_CSV, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=FIELDS).writeheader()
    if not os.path.exists(OUTPUT_FILE_JSON):
        with open(OUTPUT_FILE_JSON, "w", encoding="utf-8") as f:
            json.dump([], f, ensure_ascii=False, indent=2)


def save_json_append(path, obj):
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump([obj], f, ensure_ascii=False, indent=2)
        return
    with open(path, "r+", encoding="utf-8") as f:
        data = json.load(f)
        data.append(obj)
        f.seek(0)
        json.dump(data, f, ensure_ascii=False, indent=2)


def write_csv_row(row):
    with open(OUTPUT_FILE_CSV, "a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=FIELDS).writerow(row)


def set_watchdog_success():
    global last_success_time
    last_success_time = time.time()


def watchdog_check():
    if time.time() - last_success_time > WATCHDOG_STALL_SECONDS:
        raise TimeoutException("Watchdog: no successful scrape activity for too long.")


def extract_city_state_country(city_postal_text):
    # Example: "Agoura Hills, California, USA, 91301"
    city = state = country = ""
    if not city_postal_text:
        return city, state, country
    # Remove trailing postal if present
    parts = [p.strip() for p in city_postal_text.split(",")]
    # Heuristic: last is postal or country; find USA-like country tokens
    # Try to map by known patterns: City, State, Country [, Postal]
    if len(parts) >= 3:
        city = parts[0]
        state = parts[1]
        country = parts[2]
    elif len(parts) == 2:
        city = parts[0]
        state = parts[1]
    else:
        city = city_postal_text.strip()
    return city, state, country


def parse_property_info(modal_root):
    # Property Information and Pet/Parking sections
    data_overview = {}
    pets_blob = ""
    parking_items = []
    try:
        info_root = modal_root.find_element(By.ID, "hqv-hotel-info-section")
    except:
        return data_overview, pets_blob, parking_items

    # General check-in/out, smoke-free, accessibility
    try:
        info_cols = info_root.find_elements(By.CSS_SELECTOR, ".information-box .left-col-item .left-col-text, .information-box .left-col-item .left-col-text a")
        for col in info_cols:
            t = safe_text(col)
            if not t:
                continue
            if ":" in t:
                key = t.split(":")[0].strip()
                data_overview[key] = t
            else:
                data_overview[t] = True
    except:
        pass

    # Pet Policy (robust: handles desktop + mobile DOM)
    try:
        pet_sections = info_root.find_elements(By.CSS_SELECTOR, "div.pet-policy")
        pet_lines = []

        for section in pet_sections:
            try:
                if not section.is_displayed():
                    continue

                subinfos = section.find_elements(By.CSS_SELECTOR, ".pet-policy-sub-info")
                for s in subinfos:
                    txt = safe_text(s)
                    if txt:
                        pet_lines.append(txt)
            except:
                continue

        # Deduplicate while preserving order
        seen = set()
        pet_lines = [x for x in pet_lines if not (x in seen or seen.add(x))]

        pets_blob = " | ".join(pet_lines)
        if pets_blob:
            data_overview["Pet Policy"] = pets_blob
    except:
        pass


    # Parking
    try:
        park_section = info_root.find_element(By.CSS_SELECTOR, ".parking")
        items = park_section.find_elements(By.CSS_SELECTOR, ".parking-information .parking-item")
        for it in items:
            txt = safe_text(it)
            if txt:
                parking_items.append(txt)
        if parking_items:
            data_overview["Parking"] = "; ".join(parking_items)
    except:
        pass

    return data_overview, pets_blob, parking_items


def parse_amenities(modal_root):
    # Amenities list, including "See all amenities" toggle
    amenities = []
    try:
        amen_root = modal_root.find_element(By.ID, "hqv-amenities-section")
    except:
        return amenities

    # Click "See all amenities" if present
    try:
        see_all = amen_root.find_element(By.CSS_SELECTOR, ".see-amenities-button")
        js_click(amen_root.parent, see_all)
        time.sleep(0.7)
    except:
        pass

    try:
        items = amen_root.find_elements(By.CSS_SELECTOR, ".amenities-content .amenity-list-item .amenity-name")
        for it in items:
            name = safe_text(it)
            if name:
                amenities.append(name)
    except:
        pass
    return amenities


def parse_airports(modal_root):
    # Airport accordions under location section
    airports = []
    try:
        loc_root = modal_root.find_element(By.ID, "hqv-location-section")
    except:
        return airports

    try:
        acc_items = loc_root.find_elements(By.CSS_SELECTOR, ".accordion-item")
        for acc in acc_items:
            try:
                btn = acc.find_element(By.CSS_SELECTOR, "button.accordion-button")
                # Expand if collapsed
                aria_expanded = btn.get_attribute("aria-expanded")
                if aria_expanded == "false":
                    js_click(modal_root, btn)
                    time.sleep(0.5)

                name_el = acc.find_element(By.CSS_SELECTOR, ".location-box__location-name")
                name = safe_text(name_el)

                # After expand, try to read the collapse body for distance/shuttle if present
                # Marriott markup can vary; keep best-effort text capture
                details_texts = []
                try:
                    body_id = btn.get_attribute("data-bs-target")
                    if body_id:
                        body_sel = f"div{body_id}"
                        body = loc_root.find_element(By.CSS_SELECTOR, body_sel)
                        texts = [safe_text(x) for x in body.find_elements(By.XPATH, ".//*[normalize-space(text())]")]
                        details_texts = [t for t in texts if t]
                except:
                    pass

                airports.append({
                    "airport": name,
                    "details": details_texts[:10]  # keep compact
                })
            except:
                continue
    except:
        pass

    return airports


def open_hotel_details(driver, card_scope):
    # Two possibilities:
    # - “View Details” link/button exists -> click it
    # - Or the title button opens the modal (“hqv-modal-opener”)
    try:
        btn = card_scope.find_element(By.CSS_SELECTOR, ".view-hotel-details-section.hqv-modal-opener, .view-hotel-details-section")
        js_click(driver, btn)
        return True
    except:
        pass
    try:
        title_btn = card_scope.find_element(By.CSS_SELECTOR, ".title-container.hqv-modal-opener")
        js_click(driver, title_btn)
        return True
    except:
        return False


def wait_modal_open(driver, timeout=35):
    """
    Robust wait for Marriott hotel details modal to fully load.
    Waits for:
    - hotel info OR amenities OR location section
    - AND ensures at least one is visible
    """
    wait = WebDriverWait(driver, timeout, poll_frequency=0.25)

    def modal_ready(d):
        sections = (
            d.find_elements(By.ID, "hqv-hotel-info-section")
            + d.find_elements(By.ID, "hqv-amenities-section")
            + d.find_elements(By.ID, "hqv-location-section")
        )
        for s in sections:
            try:
                if s.is_displayed():
                    return True
            except StaleElementReferenceException:
                continue
        return False

    wait.until(modal_ready)
    time.sleep(0.6)  # allow Marriott animation/render to settle
    return True



def close_modal(driver):
    # Try ESC, or click close if visible
    try:
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
        time.sleep(0.4)
    except:
        pass
    # If still present, try to click any 'X' button commonly used
    try:
        close_btn = driver.find_element(By.CSS_SELECTOR, "button[aria-label='Close'], .modal-close, .icon-close")
        js_click(driver, close_btn)
        time.sleep(0.4)
    except:
        pass


def parse_address_block(modal_root):
    # Address and phone under location section
    address = ""
    city = state = country = ""
    phone = ""
    try:
        loc_root = modal_root.find_element(By.ID, "hqv-location-section")
        try:
            addr_block = loc_root.find_element(By.CSS_SELECTOR, ".hotel-address")
            line1 = safe_text(addr_block.find_element(By.CSS_SELECTOR, ".hotel-address-line1"))
            line2 = safe_text(addr_block.find_element(By.CSS_SELECTOR, ".hotel-address-city-postal"))
            address = (line1 + " " + line2).strip()
            c, s, co = extract_city_state_country(line2)
            city, state, country = c, s, co
        except:
            pass
        try:
            phone_el = loc_root.find_element(By.CSS_SELECTOR, ".location-box__contactNumber")
            phone = safe_text(phone_el)
        except:
            pass
    except:
        pass
    return address, city, state, country, phone


def parse_header_and_cardbits(card_scope, modal_root):
    # From card: name, rating, description; price if shown on card
    name = ""
    rating = ""
    description = ""
    price = ""
    try:
        name = safe_text(card_scope.find_element(By.CSS_SELECTOR, ".details-container .t-subtitle-xl"))
    except:
        # fallback: details header inside modal (h1/h2)
        try:
            header = modal_root.find_element(By.XPATH, ".//h1 | .//h2")
            name = safe_text(header)
        except:
            pass

    try:
        # rating number near reviews link
        rating = safe_text(card_scope.find_element(By.CSS_SELECTOR, ".ratings-value-container .star-number-container"))
    except:
        pass

    try:
        description = safe_text(card_scope.find_element(By.CSS_SELECTOR, ".description-container span"))
    except:
        # fallback: modal overview description
        try:
            desc_el = modal_root.find_element(By.CSS_SELECTOR, ".overview-description")
            description = safe_text(desc_el)
        except:
            pass

    try:
        # price on card if exposed (Marriott sometimes hides)
        price_el = card_scope.find_element(By.CSS_SELECTOR, "[data-testid='rateItem'], .price, .t-price")
        price = safe_text(price_el)
    except:
        pass

    return name, rating, description, price


def scrape_regions(driver):
    # Scrape the “Property Directory” accordion with region links on the listing page
    wait_for(driver, EC.presence_of_element_located((By.ID, "__next")), 30)
    wait_for(driver, EC.presence_of_all_elements_located((By.CSS_SELECTOR, "button.accordion__heading")), 30)

    results = []
    buttons = driver.find_elements(By.CSS_SELECTOR, "button.accordion__heading")
    for idx in range(len(buttons)):
        try:
            buttons = driver.find_elements(By.CSS_SELECTOR, "button.accordion__heading")
            btn = buttons[idx]
            country_label = btn.get_attribute("aria-label") or safe_text(btn)
            js_click(driver, btn)
            body_id = btn.get_attribute("id").replace("_heading", "_body")
            wait_for(driver, EC.presence_of_element_located((By.ID, body_id)), 20)
            body = driver.find_element(By.ID, body_id)
            links = body.find_elements(By.CSS_SELECTOR, "a.region-item-link")
            regions = []
            for a in links:
                regions.append({
                    "region": safe_text(a),
                    "url": a.get_attribute("href")
                })
            results.append({"country": country_label, "regions": regions})
        except Exception as e:
            print(f"[WARN] Region idx {idx} skipped: {e}")
            continue

    with open(OUTPUT_REGIONS_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    return results

def load_all_cards_on_page(driver, timeout=90):
    """
    Reliably load all result cards on the current results page.

    Strategy:
    - Identify the true scrollable container.
    - Read the "X - Y of Z Results" summary (if present) to get target_total.
    - Scroll to bottom repeatedly, with gentle oscillation and footer nudges,
      until we reach target_total or the count stabilizes for several rounds.
    - Periodically ping the watchdog to avoid stalls during long loads.
    """
    start = time.time()

    def get_target_total():
        try:
            # Example DOM seen:
            # <div class="results-summary"><span>1 - 28 of </span><span class="hotel-content">28 Results</span></div>
            # Your snippet shows class: sc-d88b51c4-0 ... results-summary
            summary = driver.find_element(By.CSS_SELECTOR, ".results-summary, .results-summary-container, .t-label-alt-s.results-summary, .results-summary .hotel-content, .hotel-content")
            txt = summary.text.strip()
            # Look for the trailing "Z Results"
            m = re.search(r"(\d+)\s+Results", txt, re.IGNORECASE)
            if m:
                return int(m.group(1))
        except:
            pass
        return None

    def find_scroll_container():
        # Try a set of likely containers, then fallback to documentElement
        selectors = [
            "div[data-testid='search-results']",
            "div.search-results-container",
            "div[role='region']",
            "div#search-results",
            "main",  # sometimes the main area is scrollable
        ]
        for sel in selectors:
            try:
                el = driver.find_element(By.CSS_SELECTOR, sel)
                # Heuristic: container that has or is near cards
                cards_near = el.find_elements(By.CSS_SELECTOR, ".details-container")
                if el and (cards_near or el.size.get("height", 0) > 300):
                    return el
            except:
                continue
        # Fallback: scroll the window
        return driver.find_element(By.TAG_NAME, "body")

    def get_card_count():
        try:
            return len(driver.find_elements(By.CSS_SELECTOR, ".details-container"))
        except:
            return 0

    container = find_scroll_container()
    target_total = get_target_total()  # may be None if not found

    last_count = -1
    stable_rounds = 0
    max_stable_rounds = 6  # allow more passes due to virtualization
    oscillate = 0

    # Helper: scroll to bottom of the container or window
    def scroll_to_bottom():
        try:
            driver.execute_script(
                "arguments[0].scrollTop = arguments[0].scrollHeight;", container
            )
        except:
            # Window scroll fallback
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

    # Helper: gently oscillate (up a bit then down) to trigger virtualization
    def oscillate_scroll():
        try:
            # scroll up slightly then down to bottom
            driver.execute_script(
                "arguments[0].scrollTop = Math.max(arguments[0].scrollTop - 400, 0);",
                container,
            )
        except:
            driver.execute_script(
                "window.scrollTo(0, Math.max(window.pageYOffset - 400, 0));"
            )
        time.sleep(0.25)
        scroll_to_bottom()

    # Helper: nudge footer into view (often triggers more loads)
    def nudge_footer():
        try:
            footer = driver.find_element(By.TAG_NAME, "footer")
            driver.execute_script(
                "arguments[0].scrollIntoView({block:'end'});", footer
            )
        except:
            # If no footer, just ensure bottom
            scroll_to_bottom()

    while time.time() - start < timeout:
        # Prevent watchdog from killing long loads
        set_watchdog_success()

        scroll_to_bottom()
        time.sleep(0.8)

        # small oscillations to force render of next chunk
        oscillate += 1
        if oscillate % 3 == 0:
            oscillate_scroll()
            time.sleep(0.5)

        # nudge footer occasionally
        if oscillate % 5 == 0:
            nudge_footer()
            time.sleep(0.5)

        count = get_card_count()

        # If we know the expected total, exit early when reached
        if target_total is not None and count >= target_total:
            break

        if count == last_count:
            stable_rounds += 1
        else:
            stable_rounds = 0

        # If no change for several rounds, assume we've loaded all we can
        if stable_rounds >= max_stable_rounds:
            break

        last_count = count

    # One last read after the loop
    final_count = get_card_count()
    return final_count


def iterate_cards_on_list(driver, list_url):
    driver.get(list_url)

    WebDriverWait(driver, 40).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )
    time.sleep(3)

    page_num = 1
    global_hotel_counter = 0

    while True:
        print(f"[PAGE] Scraping page {page_num}")

        total_cards = load_all_cards_on_page(driver)
        # Optional: quick second attempt if we only got ~10 but expect many
        try:
            summary_el = driver.find_element(By.CSS_SELECTOR, ".results-summary, .results-summary .hotel-content, .hotel-content")
            m = re.search(r"(\d+)\s+Results", summary_el.text.strip(), re.IGNORECASE)
            expected_total = int(m.group(1)) if m else None
        except:
            expected_total = None

        if expected_total and total_cards < min(20, expected_total):
            # Try one more load pass
            total_cards = load_all_cards_on_page(driver)
        print(f"[INFO] Found {total_cards} cards on page")

        cards = driver.find_elements(By.CSS_SELECTOR, ".details-container")
        max_cards = len(cards)

        if TEST_MODE:
            max_cards = min(TEST_CARD_LIMIT, max_cards)

        for idx in range(max_cards):
            watchdog_check()

            try:
                # ⚠️ Re-fetch cards every iteration (DOM re-renders!)
                cards = driver.find_elements(By.CSS_SELECTOR, ".details-container")
                if idx >= len(cards):
                    continue

                card = cards[idx]

                driver.execute_script(
                    "arguments[0].scrollIntoView({block:'center'});", card
                )
                time.sleep(0.7)  # allow hover + JS attach

                # Open modal
                if not open_hotel_details(driver, card):
                    print(f"[SKIP] Card {idx+1} modal not opening")
                    continue

                # ⏳ Modal load (Marriott needs 1–2s)
                wait_modal_open(driver, 30)
                time.sleep(1.8)

                modal_root = driver

                # ===== SCRAPE =====
                name, rating, description, price = parse_header_and_cardbits(card, modal_root)
                address, city, state, country, phone = parse_address_block(modal_root)
                overview, pets_blob, parking_items = parse_property_info(modal_root)
                amenities = parse_amenities(modal_root)
                airports = parse_airports(modal_root)
                property_website = parse_property_website(modal_root)

                is_pet = (
                    "pet" in (json.dumps(overview, ensure_ascii=False) + " " + " ".join(amenities)).lower()
                    or "pet friendly" in (description or "").lower()
                    or bool(pets_blob)
                )

                hotel_code = f"MAR-{page_num}-{idx+1}"

                row = {
                    "hotel_code": hotel_code,
                    "hotel_name": name,
                    "address": address,
                    "city": city,
                    "state": state,
                    "country": country,
                    "phone": phone,
                    "rating": rating,
                    "description": description,
                    "card_price": price,
                    "property_website": property_website,
                    "overview_table_json": json.dumps(overview, ensure_ascii=False),
                    "pets_json": json.dumps({"raw": pets_blob} if pets_blob else {}, ensure_ascii=False),
                    "parking_json": json.dumps({"items": parking_items} if parking_items else {}, ensure_ascii=False),
                    "amenities_json": json.dumps(amenities, ensure_ascii=False),
                    "nearby_json": json.dumps([], ensure_ascii=False),
                    "airport_json": json.dumps(airports, ensure_ascii=False),
                    "is_pet_friendly": "true" if is_pet else "false",
                    "last_updated": datetime.utcnow().isoformat(),
                }

                write_csv_row(row)
                save_json_append(OUTPUT_FILE_JSON, row)
                set_watchdog_success()

                print(f"[OK] {name}")

            except StaleElementReferenceException:
                print(f"[RETRY] Card {idx+1} stale — retrying")
                continue

            except Exception as e:
                print(f"[WARN] Card {idx+1} error: {e}")

            finally:
                close_modal(driver)
                time.sleep(0.9)  # ⏳ allow grid re-render

        if TEST_MODE:
            print("[TEST MODE] Stopping after first page")
            return

        # ===== PAGINATION =====
        try:
            next_btn = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "a[aria-label='NextPage']")
                )
            )

            if "disabled" in next_btn.get_attribute("class"):
                print("[DONE] No more pages")
                break

            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", next_btn)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", next_btn)

            page_num += 1
            time.sleep(3)

        except Exception as e:
            print(f"[END] Pagination failed: {e}")
            break



def main():
    ensure_output_files()
    driver = uc.Chrome(options=make_options(), use_subprocess=True)
    try:
        driver.get(START_URL)
        wait_for(driver, EC.presence_of_element_located((By.ID, "__next")), 40)
        wait_for(driver, EC.presence_of_all_elements_located((By.CSS_SELECTOR, "button.accordion__heading")), 40)

        print("[INFO] Scraping region list...")
        regions = scrape_regions(driver)
        print(f"[INFO] Regions found: {sum(len(x['regions']) for x in regions)}")

        # Iterate each region link -> within it, run pagination and cards
        for country_entry in regions:
            for reg in country_entry.get("regions", []):
                url = reg.get("url")
                if not url:
                    continue
                print(f"[PAGE] {country_entry.get('country')} -> {reg.get('region')} -> {url}")
                try:
                    iterate_cards_on_list(driver, url)
                except Exception as e:
                    print(f"[WARN] Failed region page {url}: {e}")
                    continue

        print("[DONE] Marriott scraping completed.")

    finally:
        driver.quit()


if __name__ == "__main__":
    # Safety loop with auto-retry on watchdog stall
    while True:
        try:
            main()
            break
        except Exception as e:
            print(f"[RESTART] Error: {e}. Restarting in 10s...")
            time.sleep(10)